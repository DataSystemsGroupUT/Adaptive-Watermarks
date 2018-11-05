package ee.ut.cs.dsg.adaptivewatermark.flink.counters;

import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public  class CounterFunction implements AllWindowFunction<SimpleEvent, Tuple3<TimeWindow,Long, Long>, TimeWindow>
{

    private long watermark;

    public CounterFunction(long wm)
    {
        this.watermark = wm;
    }

    @Override
    public void apply(TimeWindow window, Iterable<SimpleEvent> iterable, Collector<Tuple3<TimeWindow, Long, Long>> collector) throws Exception {

        long count = 0L;
        for (SimpleEvent e : iterable)
        {
            count++;
        }

        //System.out.println("Num elements in Window ("+window.getStart()+","+window.getEnd()+") is "+count);
        collector.collect(new Tuple3<>(window, Long.valueOf(count), watermark - window.getEnd() ));
    }
}
