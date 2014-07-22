package org.apache.zookeeper;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;

public class Config {

    protected Properties properties;
    protected int writes = 10;
    protected int size = 1024;
    protected String connect = "localhost:2181";
    protected int clients = 1;
    protected int samples = 5000;
    protected boolean cleanup = true;
    protected ArrayList<EventDescription> eventList = new ArrayList<EventDescription>();
    Iterator<EventDescription> eventIterator;
    
    public Config(String configFile) 
    throws IOException {
        FileInputStream fis = new FileInputStream(configFile);
        properties = new Properties(System.getProperties());
        properties.load(fis);
        fis.close();
    }
    
    public Config() {
        this.properties = new Properties(System.getProperties());
    }
    
    public void loadConfig() {
        writes = Integer.decode(properties.getProperty("writes", "10"));
        size = Integer.decode(properties.getProperty("size", "1024"));
        connect = properties.getProperty("connect", "localhost:2181");
        clients = Integer.decode(properties.getProperty("clients", "1")).intValue();
        samples = Integer.decode(properties.getProperty("samples", "5000")).intValue();
        cleanup = Boolean.getBoolean(properties.getProperty("cleanup", "true"));
            
        /*
         * Dealing with events
         */
        String eventString = properties.getProperty("events", "");
        String[] events = eventString.split(";");
        System.out.println("Event length: " + events.length);
        if(!eventString.isEmpty()){
            for( int i = 0; i < events.length; i++) {
                eventList.add(new EventDescription(events[i]));
            }
        } else {
            eventList.add(new EventDescription(new String(this.writes + ":" + this.size)));
        }
       
        eventIterator = eventList.iterator();
    }
    
    public int getWrites() {
        return writes;
    }
    
    public int getSize() {
        return size;
    }
    
    public String getConnect() {
        return connect;
    }
    
    public int getClients() {
        return clients;
    }
    
    public int getSamples() {
        return samples;
    }
    
    public boolean needsCleanup() {
        return cleanup;
    }
   
    public EventDescription nextEvent() {
        if(eventIterator.hasNext()) {
            return eventIterator.next();
        }
        
        return null;
    }
}
