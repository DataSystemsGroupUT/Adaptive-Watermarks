package ee.ut.cs.dsg.adaptivewatermark.flink.source;

import ee.ut.cs.dsg.adaptivewatermark.AdaptiveWatermarkEstimator;
import ee.ut.cs.dsg.adaptivewatermark.PeriodicWaterMarkEstimator;
import ee.ut.cs.dsg.adaptivewatermark.WatermarkEstimator;
import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;


import java.io.BufferedReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class AdaptiveWatermarkGeneratorSource implements SourceFunction<SimpleEvent> {

    private static final long serialVersionUID = -2873892890991630938L;
    private boolean running=true;
    private String filePath;

    private long watermarkPeriod;
    private long maxAllowedLateness;


    private double OOOThreshold;
    private double sensitivity;
    private long totalOOOArrival=0;
    private long totalElements =0;
    private Random random = new Random();
    long currentWatermark=0;

    private  Random delay ;
    WatermarkEstimator estimator;
    private WatermarkEstimator.ReferenceEventType referenceEventType;
    SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private long warmupTuplesCount=1000;
    protected long numberOfGeneratedWatermarks=0;

    public long getNumberOfGeneratedWatermarks(){return numberOfGeneratedWatermarks;}

    public AdaptiveWatermarkGeneratorSource(String path)
    {
        filePath = path;
        watermarkPeriod = 1000;
        maxAllowedLateness = 1000;
        OOOThreshold = 1.1;
        sensitivity = 1.0;
        estimator = new AdaptiveWatermarkEstimator();
    }
    public AdaptiveWatermarkGeneratorSource(String path, long maxLateness, double oooThrshld, double sns)
    {
        filePath = path;
        watermarkPeriod = 1000;
        maxAllowedLateness = maxLateness;
        OOOThreshold = oooThrshld;
        sensitivity = sns;
        estimator = new AdaptiveWatermarkEstimator(sns, 1.0, oooThrshld, maxLateness);
    }
    public AdaptiveWatermarkGeneratorSource(String path, long watermarkPeriod, long maxAllowedLateness)
    {
        filePath = path;
        this.watermarkPeriod = watermarkPeriod;
        this.maxAllowedLateness = maxAllowedLateness;
        OOOThreshold = 1.1;
        sensitivity = 1.0;
        //estimator = new PeriodicWaterMarkEstimator(watermarkPeriod, maxAllowedLateness);
        estimator = new AdaptiveWatermarkEstimator(sensitivity, 1.0,OOOThreshold,maxAllowedLateness);
    }
    public AdaptiveWatermarkGeneratorSource(String path, long maxAllowedLateness, double oooThreshold, double sensitivity, double sensitivityChangeRate)
    {
        filePath = path;
        this.maxAllowedLateness = 1000;
        delay  =new Random();
        estimator = new AdaptiveWatermarkEstimator(sensitivity,sensitivityChangeRate,oooThreshold,maxAllowedLateness);
    }
    public long getCurrentWatermark()
    {
        return currentWatermark;
    }
    @Override
    public void run(SourceContext<SimpleEvent> sourceContext) throws Exception
    {
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            line = reader.readLine();
            // the following play with timestamps is jut to put it near to the current time as some data are from 2013
            long firstIngestionTime = System.currentTimeMillis();
            long firstTimestamp = 0;
            long transmissionDelay=2;
            while (running && line != null)
            {
                totalElements++;
                SimpleEvent se;
                long ts; double temperature;
                String[] data = line.split(",");

                if (data.length == 2)
                {
                    ts = Long.parseLong(data[0]);
                    temperature = Double.parseDouble(data[1]);
                }
                else
                {
                    ts = Long.parseLong(data[0]);
                    temperature = Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;
                }

                if (firstTimestamp == 0)
                {
                    firstTimestamp = ts;
                }

            //    se = new SimpleEvent(firstIngestionTime + (ts - firstTimestamp) , temperature);

                se = new SimpleEvent(ts  , temperature, "1");

                sourceContext.collectWithTimestamp(se, se.getTimestamp());
                int randomDelay = delay.nextInt((int)maxAllowedLateness);

                if (estimator.processEvent(se.getTimestamp(), se.getTimestamp() /*System.currentTimeMillis()*/+randomDelay))
                {
                    // We need to generate the watermark
              //      if (estimator.getWatermark() > currentWatermark && currentWatermark != 0)
                 //       System.out.println("Pending windows "+((estimator.getWatermark() - currentWatermark)/100));

                    if (estimator.getWatermark() > currentWatermark) {
                        numberOfGeneratedWatermarks++;
                        currentWatermark = estimator.getWatermark();
                        Watermark wm = new Watermark(estimator.getWatermark());

                        sourceContext.emitWatermark(wm);
                        System.out.println("Generating a new watermark with timestamp ("+wm.getTimestamp()+")" + sdfDate.format(new Date(wm.getTimestamp())));
                        System.out.println("Total number of generated watermarks "+this.getNumberOfGeneratedWatermarks());
                    }
                }

                if (estimator.wasLastElementALateArrival())
                {
                    System.out.println("\t Arrival of an event "+ se.toString()+" behind the watermark "+estimator.getWatermark());
                    totalOOOArrival++;
                }
                if (estimator instanceof PeriodicWaterMarkEstimator)
                {
                    Thread.sleep(1000,1);
                }
                line = reader.readLine();
            }
            System.out.println("Total OOO Arrival "+totalOOOArrival +" of total elements "+totalElements +" with percentage "+(double)totalOOOArrival/totalElements);
            if (estimator instanceof PeriodicWaterMarkEstimator)
            {
                ((PeriodicWaterMarkEstimator)estimator).terminate();
            }
            reader.close();
        }
        catch (IOException ioe)
        {
            ioe.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        running = false;
        if (estimator instanceof PeriodicWaterMarkEstimator)
        {
            ((PeriodicWaterMarkEstimator)estimator).terminate();
        }
    }
}
