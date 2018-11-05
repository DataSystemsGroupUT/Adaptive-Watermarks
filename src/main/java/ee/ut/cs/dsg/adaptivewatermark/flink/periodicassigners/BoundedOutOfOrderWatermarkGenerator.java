package ee.ut.cs.dsg.adaptivewatermark.flink.periodicassigners;

import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Date;

import java.text.SimpleDateFormat;

public class BoundedOutOfOrderWatermarkGenerator implements AssignerWithPeriodicWatermarks<SimpleEvent> {


    private long maxOutOfOrderness = 3500; // 3.5 seconds
    private long numberOfGeneratedWatermarks=0;
    public BoundedOutOfOrderWatermarkGenerator(long maxOOO)
    {
        this.maxOutOfOrderness = maxOOO;
    }

    public long totalElements=0,totalOOOElements=0;
    private long currentMaxTimestamp;
    long currentWatermark=0;
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    @Override
    public long extractTimestamp(SimpleEvent element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp();
        if (timestamp < currentWatermark) {
            totalOOOElements++;
            System.out.println("\t Arrival of an event " + element.toString() + " behind the watermark " + currentWatermark);

        }
        totalElements++;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);

        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        long nextWatermark = currentMaxTimestamp - maxOutOfOrderness;
//        if (currentWatermark != 0)
//            System.out.println("Pending windows "+ ((nextWatermark-currentWatermark)/100));
        if (nextWatermark > currentWatermark) {
            numberOfGeneratedWatermarks++;
            currentWatermark = nextWatermark;
            System.out.println("Generating a new watermark with timestamp (" + nextWatermark + ")" + sdfDate.format(new Date(nextWatermark)));
            System.out.println("Total number of generated watermarks "+this.getNumberOfGeneratedWatermarks());
            return new Watermark(nextWatermark);
        }
       // System.out.println("Next watermark "+nextWatermark);
        System.out.println("Total OOO Arrival "+totalOOOElements+" of total elements "+totalElements +" with percentage "+(double)totalOOOElements/totalElements);
        return null;
    }

    public long getNumberOfGeneratedWatermarks(){return numberOfGeneratedWatermarks;}
}