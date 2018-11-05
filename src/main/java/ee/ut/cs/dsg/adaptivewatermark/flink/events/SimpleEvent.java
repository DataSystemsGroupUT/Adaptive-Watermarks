/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ee.ut.cs.dsg.adaptivewatermark.flink.events;

/**
 *
 * @author Ahmed Awad
 */
public class SimpleEvent {
    
    private long timestamp;
    private double temperature;
    private String key;
    public SimpleEvent()
    {
        timestamp = System.currentTimeMillis();
    }
    public SimpleEvent(long ts)
    {
        timestamp = ts;
    }
    public SimpleEvent(long ts, double temp, String key)
    {
        timestamp = ts;
        temperature = temp;
        this.key = key;
    }
    public long getTimestamp()
    {
        return timestamp;
    }
    public void setTimestamp(long ts)
    {
        timestamp = ts;
    }
    public String getKey(){return key;}
    public double getTemperature(){return temperature;}

    public void setTemperature(double t)
    {
        temperature = t;
    }
    @Override
    public String toString()
    {
        return "SimpleEvent("+this.timestamp+","+this.temperature+")";
    }
    
}
