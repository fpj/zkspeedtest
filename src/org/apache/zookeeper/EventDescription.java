package org.apache.zookeeper;

public class EventDescription {
    public static final int STOPEVENT = -1;
    
    public int delimiter;
    public int size;
    
    public EventDescription(String event) 
    throws IllegalArgumentException{
        String[] pair = event.split(":");
        if(pair.length != 2) {
            System.out.println("Configuration error: " + event);
            throw new IllegalArgumentException();
        }
        
        delimiter = Integer.decode(pair[0]);
        size = Integer.decode(pair[1]);
    }
}
