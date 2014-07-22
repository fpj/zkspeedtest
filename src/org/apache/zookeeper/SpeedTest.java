package org.apache.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SpeedTest implements Watcher{
    
    Config config;
    protected String connectString;
    protected boolean connected = false;
    protected ZooKeeper zk;
    protected StringBuilder rootString = new StringBuilder();
    protected CountDownLatch latch, overarchingLatch;
    protected int writeCount = 0;
    
    public SpeedTest(String configFile) {
           
    }
    
    public SpeedTest() 
    throws IOException {
        String confFile = System.getProperty("conf");
        if( confFile != null) {
            this.config = new Config(confFile);
        } else {
            this.config = new Config();
        }
        
        this.config.loadConfig();
    }
    
    /*public SpeedTest(int numOfZnodes) {
        this.numOfZnodes = numOfZnodes;
    }
    
    public SpeedTest(String connectString,
                        int clients,
                        int numOfZnodes, 
                        int size,
                        int samples) {
        this.connectString = connectString;
        this.clients = clients;
        this.numOfZnodes = numOfZnodes;
        this.size = size;
        this.samples = samples;
    }*/
    
    @Override
    public void process(WatchedEvent event) {
        switch(event.getState()) {
        case SyncConnected:
            connected = true;
            break;
        case Disconnected:
        case Expired:
            connected = false;
            System.out.println("Not connected");
        default:
            System.out.println("Received event: " + event.getState());
        }
    }
    
    ZooKeeper init() 
    throws IOException {
        this.zk = new ZooKeeper(config.getConnect(), 10000, this);  
        
        return zk;
    }

    void runWrite() {
        populate();
    }
    
    void populate() {
        overarchingLatch = new CountDownLatch(1);
        zk.create("/test-",
                    new byte[0],
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL,
                    rootCb,
                    null);
    }
    
    long lastWrite;
    StringCallback rootCb = new StringCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    String name) {
            switch(Code.get(rc)) {
            case OK:
                rootString.append(name);
                lastWrite = System.currentTimeMillis();
                
                new Thread() {
                    public void run() {
                        long begin;
                        EventDescription event = config.nextEvent();
                        while(event != null) {
                            begin = System.currentTimeMillis();
                            latch = new CountDownLatch(event.delimiter);
                        
                            StringBuilder rootBuilder = new StringBuilder();
                            byte[] data = new byte[event.size];
                            int j = 0;
                            rootBuilder.append(rootString + "/test-");
                            String base = rootBuilder.toString() + j;
                            for( int i = 0; i < event.delimiter; i++ ) {
                                if(!connected) {
                                    System.out.println("Stop sending requests, not connected");
                                    return;
                                }
                            
                                if((i % 10000) == 0) {
                                    base = rootBuilder.toString() + j;
                                    zk.create(base, 
                                            data,
                                            Ids.OPEN_ACL_UNSAFE, 
                                            CreateMode.PERSISTENT,
                                            nullCb,
                                            null);
                                    j++;
                                }
                                zk.create(base + "/data-", 
                                        data,
                                        Ids.OPEN_ACL_UNSAFE, 
                                        CreateMode.PERSISTENT_SEQUENTIAL,
                                        stringCb,
                                        null);
                            }
                        
                            /*
                             * Waits for all responses before starting the
                             * next stage.
                             */
                            try{
                                latch.await(event.delimiter*15, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException e) {
                                System.out.println("Interrupted while waiting for stage to finish");
                            }
                            
                            long end = System.currentTimeMillis();
                            long diff = end - begin;
                            System.out.println("Write time: " + diff + ", " + event.delimiter);
                            writeCount += event.delimiter;
                            
                            event = config.nextEvent();
                        }
                        
                        overarchingLatch.countDown();
                    }
                }.start();
                
                break;
            default:
                System.out.println("ZK operation went wrong" + path);
            }
        }
    };
    
    StringCallback nullCb = new StringCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    String name) {
            switch(Code.get(rc)) {
            case OK:            
                break;
            default:
                break;
            }        
        }
    };
    
    StringCallback stringCb = new StringCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    String name) {
            switch(Code.get(rc)) {
            case OK:
                latch.countDown();
                if((latch.getCount() % config.samples) == 0){
                   long current = System.currentTimeMillis();
                   long time = current - lastWrite;
                   System.out.println("Writes " + latch.getCount() + ": " + time); 
                   lastWrite = current;
                }
                
                break;
            default:
                System.out.println("Error when processing callback: " + path + ", " + rc);
                break;
            }        
        }
    };
    
    void runRead() {
        overarchingLatch = new CountDownLatch(writeCount) ;
        zk.getChildren(rootString.toString(), 
                           false,
                           childrenCb,
                           null);
    }
    
    ChildrenCallback childrenCb = new ChildrenCallback() {
        @Override
        public void processResult(int rc, 
                             String path, 
                             Object ctx, 
                             final List<String> children) {
            switch(Code.get(rc)) {
            case OK:
                new Thread() {
                    public void run() {
                        for(String child: children) {
                            zk.getChildren(rootString + "/" + child,
                                        null,
                                        parentCb,
                                        null);
                        }
                    }
                }.start();
                break;
            default:
                break;        
            }
        }
    };

    volatile int counter = 0;
    ChildrenCallback parentCb = new ChildrenCallback() {
        @Override
        public void processResult(int rc, 
                                    final String path, 
                                    Object ctx, 
                                    final List<String> children) {
            switch(Code.get(rc)) {
            case OK:
                new Thread() {
                    public void run() {
                        for(String child: children) {
                            zk.getData(path + "/" + child,
                                        null,
                                        dataCb,
                                        null);
                        }
                    }
                    }.start();
                    break;
            default:
                System.out.println("Something has gone wrong: " + path + ", " + rc);
            }
        }
    };
    
    DataCallback dataCb = new DataCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    byte[] data, 
                                    Stat stat) {
            switch(Code.get(rc)) {
            case OK:
                if(counter++ % 10000 == 0){
                    System.out.println(path);
                }
                overarchingLatch.countDown();
                break;
            default:
                System.out.println("Something got wrong: " + path + ", " + rc);
            }
        }
    };
    
    boolean await(long timeout) 
    throws InterruptedException {
        return overarchingLatch.await(timeout, TimeUnit.MILLISECONDS);
    }
    
    void cleanUp() {
        if(config.needsCleanup()) {
            return;    
        }
        
        try {
            StringBuilder rootBuilder = new StringBuilder();
            rootBuilder.append(rootString + "/");
            List<String> children = zk.getChildren(rootString.toString(), false);
            for (String name : children) {
                List<String> subChildren = zk.getChildren(rootString.toString() + "/" + name, false);
                for(String child : subChildren){
                    zk.delete( rootBuilder.toString() + name + "/" + child, -1, delCb, null);  
                }
                zk.delete(rootBuilder.toString() + name, -1, delCb, null);
            }
            zk.delete(rootString.toString(), -1, delCb, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    VoidCallback delCb = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch(Code.get(rc)) {
            case OK:
                break;
            default:
                System.out.println("Something went wrong: " + path + ", " + rc);
            }
        }
    };
    
    /*
     * Setup and run barrier
     */
    CountDownLatch barrier = new CountDownLatch(1);
    void setUpBarrier()
    throws InterruptedException {
        zk.create("/barrier",
                 new byte[0],
                 Ids.OPEN_ACL_UNSAFE,
                 CreateMode.PERSISTENT,
                 barrierParentCb,
                 null);
        barrier.await();
        System.out.println("Crossed the barrier");
    }
    
    StringCallback barrierParentCb = new StringCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    String name) {
            switch(Code.get(rc)) {
            case OK:
            case NODEEXISTS:
                zk.create("/barrier/barrier-",
                        new byte[0],
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL,
                        barrierNodeCb,
                        null);
                break;
            default:
                System.out.println("Something went wrong: " + path + ", " + rc);
            }
            
            System.out.println("Creating barrier node");
        }
    };
    
    String myBarrier = null;
    StringCallback barrierNodeCb = new StringCallback() {
        @Override
        public void processResult(int rc, 
                                    String path, 
                                    Object ctx, 
                                    String name) {
            switch(Code.get(rc)) {
            case OK:
                myBarrier = name;
                zk.getChildren("/barrier", 
                                 childrenWatcher,
                                 barrierChildrenCb,
                                 null);
                break;
            default:
                System.out.println("Something went wrong: " + path + ", " + rc);
            }
            
            System.out.println("Getting children");
        }
    };
      
    Watcher childrenWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == EventType.NodeChildrenChanged) {
                zk.getChildren("/barrier",
                        childrenWatcher,
                        barrierChildrenCb,
                        null);
            }
        }
    };
    
    ChildrenCallback barrierChildrenCb = new ChildrenCallback() {
      @Override
      public void processResult(int rc, 
                                  String path, 
                                  Object ctx, 
                                  List<String> children) {
          switch(Code.get(rc)) {
              case OK:
                  if(children.size() == config.getClients()) {
                      barrier.countDown();
                  }
                  
                  break;
              default:
                  System.out.println("Something went wrong: " + path + ", " + rc);  
          }       
      }
    };
    
    void releaseBarrier()
    throws KeeperException, InterruptedException {
        zk.delete(myBarrier, -1);
        List<String> children = zk.getChildren("/barrier", false);
        if(children.size() == 0) {
            zk.delete("/barrier", -1);
        }
    }
    
    public void close()
    throws InterruptedException {
        zk.close();
    }
    
    static void printHelp() {
        System.out.println("Use: java -cp ... SpeedTest \\ ");
        System.out.println("\t -Dconnect=<connect string>");
        System.out.println("\t -Dwritest=<numer of znodes>");
        System.out.println("\t -Dsize=<znode size>");
        System.out.println("\t -Dclients=<number of clients>");
        System.out.println("\t -Dcleanup=[true|false]");
    }
    
    public static void main (String[] args) {

        SpeedTest test = null; 
        
        try{
            if(args.length > 0) {
                printHelp();
            }
            
            test = new SpeedTest();
            /*if(System.getProperty("config") != null) {
                test = new SpeedTest(System.getProperty("config"));
            } else {
                String connectString = System.getProperty("connect", "localhost:2181");
                int numOfZnodes = Integer.getInteger("writes", 500000);
                int size = Integer.getInteger("size", 1024);
                boolean cleanup = Boolean.getBoolean("cleanup");
                int clients = Integer.getInteger("clients", 1);
                int samples = Integer.getInteger("samples", 5000);
            
                test = new SpeedTest(connectString, 
                                        clients, 
                                        numOfZnodes, 
                                        size, 
                                        samples);
            } */
        
            ZooKeeper zk = test.init();
            test.setUpBarrier();
            
            while(!test.connected) {
                Thread.sleep(100);
            }
        
            System.out.println("Initializing");
            
            test.runWrite();
            boolean finished = test.await(100000);
            if(!finished){
                System.out.println("Didn't finish writing");
            }
            
            System.out.println("Reading");
            long begin = System.currentTimeMillis();
            test.runRead();
            test.await(100000);
            long end = System.currentTimeMillis();
            long diff = end - begin;
            
            System.out.println("Total time: " + diff);
           
            test.cleanUp();
            List<String> list = zk.getChildren("/", false);
            // Should contain a single item, zookeeper
            System.out.println("After cleanup: " + list.size());
            
            test.releaseBarrier();
            
            System.out.println("Done");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(test != null) {
                try {
                    test.close();
                } catch (InterruptedException e) {
                    System.out.println("Interrupted while closing");
                }
            }
        }
    }
}
