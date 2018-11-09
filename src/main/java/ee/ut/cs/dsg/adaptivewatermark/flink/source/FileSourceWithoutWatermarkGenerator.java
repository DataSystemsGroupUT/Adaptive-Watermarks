package ee.ut.cs.dsg.adaptivewatermark.flink.source;


import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.util.Random;

public class FileSourceWithoutWatermarkGenerator implements SourceFunction<SimpleEvent> {


    private boolean running=true;
    private String filePath;



    private Random random = new Random();





    public FileSourceWithoutWatermarkGenerator(String path)
    {
        filePath = path;

    }

    @Override
    public void run(SourceContext<SimpleEvent> sourceContext) throws Exception
    {
        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            line = reader.readLine();
            long firstIngestionTime = System.currentTimeMillis();
            long firstTimestamp = 0;
            while (running && line != null)
            {
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
                se = new SimpleEvent(firstIngestionTime + (ts - firstTimestamp), temperature, "1");

                sourceContext.collect(se);

                line = reader.readLine();
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
    }
}
