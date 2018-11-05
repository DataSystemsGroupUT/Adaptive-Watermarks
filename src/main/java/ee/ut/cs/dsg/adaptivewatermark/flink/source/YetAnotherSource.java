package ee.ut.cs.dsg.adaptivewatermark.flink.source;

import ee.ut.cs.dsg.adaptivewatermark.flink.events.SimpleEvent;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import sun.nio.cs.ArrayEncoder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class YetAnotherSource implements SourceFunction<Tuple3<Long,String,Double>> {

    private boolean running=true;
    private String filePath;
    private Random random = new Random();

    public YetAnotherSource(String file)
    {
        filePath = file;
    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Double>> sourceContext) throws Exception {

        try
        {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;
            line = reader.readLine();
            while (running && line != null)
            {
                if (line.startsWith("*"))
                {
                    line = reader.readLine();
                    continue;
                }

                long ts; double temperature; String key;
                String[] data = line.split(",");

                if (data.length == 3)
                {
                    ts = Long.parseLong(data[0]);
                    temperature = Double.parseDouble(data[2]);
                    key = data[1];
                }
                else
                {
                    ts = Long.parseLong(data[0]);
                    temperature = Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;
                    key = "DUMMY";
                }

                if (key.equals("W")) // This is a watermark timestamp
                {
                    sourceContext.emitWatermark(new Watermark(ts));
                }
                else
                {
                    sourceContext.collectWithTimestamp( new Tuple3<>(ts,key,temperature), ts);
                }



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
