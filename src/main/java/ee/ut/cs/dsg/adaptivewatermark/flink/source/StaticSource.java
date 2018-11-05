package ee.ut.cs.dsg.adaptivewatermark.flink.source;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class StaticSource implements SourceFunction<Tuple3<Long,String,Double>> {

    private boolean running=true;
//    private String filePath;
    private Random random = new Random();

//    public StaticSource(String file)
//    {
//        filePath = file;
//    }

    @Override
    public void run(SourceContext<Tuple3<Long, String, Double>> sourceContext) throws Exception {

//        try
//        {
//            long ts; double temperature; String key;


//            BufferedReader reader = new BufferedReader(new FileReader(filePath));
//            String line;
//            line = reader.readLine();
//            while (running && line != null)
//            {
//
//
//                String[] data = line.split(",");
//
//                if (data.length == 3)
//                {
//                    ts = Long.parseLong(data[0]);
//                    temperature = Double.parseDouble(data[2]);
//                    key = data[1];
//                }
//                else
//                {
//                    ts = Long.parseLong(data[0]);
//                    temperature = Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0;
//                    key = "DUMMY";
//                }


            //This is intended for a session window of gap 100 milli
            sourceContext.collectWithTimestamp( new Tuple3<>(10L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 10L);
            sourceContext.collectWithTimestamp( new Tuple3<>(30L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 15L);
            sourceContext.collectWithTimestamp( new Tuple3<>(70L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 70L);
          //  sourceContext.emitWatermark(new Watermark(70L));

            sourceContext.collectWithTimestamp( new Tuple3<>(200L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 200L);
            sourceContext.collectWithTimestamp( new Tuple3<>(180L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 195L);
//            sourceContext.collectWithTimestamp( new Tuple3<>(110L,"Dummy",Math.round(((random.nextGaussian()*5)+20)*100.0)/100.0), 40L);
//            sourceContext.emitWatermark(new Watermark(250L));

//            }
//            reader.close();
//        }
//        catch (IOException ioe)
//        {
//            ioe.printStackTrace();
//        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
