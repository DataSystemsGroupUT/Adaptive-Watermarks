package ee.ut.cs.dsg.adaptivewatermark.flink.periodicassigners;


import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import org.apache.flink.streaming.api.watermark.Watermark;


public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<SimpleEvent> {

    private final long maxTimeLag = 100; // 5 seconds

    @Override
    public long extractTimestamp(SimpleEvent element, long previousElementTimestamp) {
        return element.getTimestamp();
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current time minus the maximum time lag
        long nextWatermark = System.currentTimeMillis() - maxTimeLag;
        System.out.println("Next watermark "+nextWatermark);
        return new Watermark(nextWatermark);
    }
}