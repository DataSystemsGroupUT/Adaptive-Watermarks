package ee.ut.cs.dsg.resultanalysis;

import java.io.BufferedReader;
import java.io.FileReader;

public class ResultAnalyzer {

    public static void main(String[] args) throws Exception{
        String[] files = new String[28];

        files[0] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenPeriodicL-2000P-2000W-1000.txt" ;
        files[1] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-2000OOO-0.01S-0.01SCR-1.0W-1000.txt";
//        files[0] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-2000OOO-0.01S-0.01SCR-1.0W-1000.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-2000P-2000W-1000.txt";
//        files[0] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-1000P-2000W-1000.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-1000P-200W-1000.txt";

//        files[0] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-0.1W-100.txt";
//        files[0] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-1000P-200W-100.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt";

//       files[0] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-1000P-200W-100.txt";
//       files[1] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.5S-1.0SCR-0.1W-100.txt";

//        files[0]= "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondPeriodicL-100P-10W-100.txt";

//        files[0] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenPeriodicL-100P-10W-100.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[2] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-1000.txt";
//        files[3] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-1000.txt";
//        files[4] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-1000.txt";
//        files[5] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-1000.txt";
//        files[6] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliPeriodicL-100P-10W-1000.txt";
//        files[7] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-100OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[8] = "C:\\Work\\Data\\Tromso2013MillisecondPeriodicL-100P-10W-1000.txt";
//        files[9] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-100OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[10] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondPeriodicL-100P-10W-1000.txt";
//        files[11] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-100OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[12] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenPeriodicL-100P-10W-1000.txt";
//        files[13] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-100OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[14] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliPeriodicL-100P-10W-100.txt";
//        files[15] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[16] = "C:\\Work\\Data\\Tromso2013MillisecondPeriodicL-100P-10W-100.txt";
//        files[17] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[18] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondPeriodicL-100P-10W-100.txt";
//        files[19] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-100OOO-0.1S-1.0SCR-0.1W-100.txt";

//        files[0] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-1000.txt";
//        files[1] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenPeriodicL-1000P-200W-1000.txt";
//        files[2] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-1000.txt";
//
//        files[3] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-100.txt";
//        files[4] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-100.txt";
//        files[5] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-100.txt";
//        files[6] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-100.txt";
//        files[7] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliPeriodicL-1000P-200W-100.txt";
//        files[8] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt";
//
//        files[9] = "C:\\Work\\Data\\Tromso2013MillisecondPeriodicL-1000P-200W-100.txt";
//        files[10] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[11] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondPeriodicL-1000P-200W-100.txt";
//        files[12] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[13] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenPeriodicL-1000P-200W-100.txt";
//        files[14] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt";
//        files[15] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-1000OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[16] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-1000OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[17] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondAdaptiveL-1000OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[18] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-0.5S-1.0SCR-0.1W-1000.txt";
//        files[19] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliPeriodicL-1000P-200W-1000.txt";
//        files[20] = "C:\\Work\\Data\\DEBSGC2012TSOnlyMilliAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-1000.txt";
//        files[21] = "C:\\Work\\Data\\Tromso2013MillisecondPeriodicL-1000P-200W-1000.txt";
//        files[22] = "C:\\Work\\Data\\Tromso2013MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-1000.txt";
//        files[23] = "C:\\Work\\Data\\SensorTimeStampRealCompleteMillisecondPeriodicL-1000P-200W-1000.txt";
        int k = 1;
        System.out.println(AnalysisResult.getHeader());

        for (int i = 0; i <= k;i++)
        {
            AnalysisResult result = analyzeFile(files[i]);
            System.out.println(result.toString());
        }
    }

    private static AnalysisResult analyzeFile(String file) throws Exception
    {
        AnalysisResult result=null;
        double elementsCount=0;
        long numElements = 0;
        long numWindows = 0;
        String datasetName;
        long allowedLateness=0;
        long period = 0;
        double sensitivity=0F;
        double sensitivityChangeRate = 1.1;
        boolean isAdaptive;
        double oooThreshold = 1.1F;
        long windowWidth=0;
        double averageLag = 0;


        if (file.indexOf("Adaptive") != -1)
        {
            isAdaptive = true;
            String [] splits = file.split("Adaptive");
            datasetName = splits[0];
            String[] splits2 = splits[1].split("-");
            allowedLateness = Long.valueOf(splits2[1].replace("OOO",""));
            oooThreshold = Double.valueOf(splits2[2].replace("S",""));
            sensitivity = Double.valueOf(splits2[3].replace("SCR",""));
            sensitivityChangeRate = Double.valueOf(splits2[4].replace("W",""));
            windowWidth = Long.valueOf(splits2[5].replace(".txt",""));
        }
        else
        {
            isAdaptive = false;
            String [] splits = file.split("Periodic");
            datasetName = splits[0];
            String[] splits2 = splits[1].split("-");
            allowedLateness = Long.valueOf(splits2[1].replace("P",""));
            period = Long.valueOf(splits2[2].replace("W",""));
            windowWidth = Long.valueOf(splits2[3].replace(".txt",""));

        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        line = reader.readLine();

        while (line != null)
        {
            String[] content = line.split(",");
            numElements+= Long.valueOf(content[2].trim());
            numWindows++;
            averageLag+= Double.valueOf(content[3].replace(")","").trim());
            line = reader.readLine();
        }
        result = new AnalysisResult(datasetName,isAdaptive,numWindows,averageLag/numWindows,numElements
        ,allowedLateness,sensitivity,sensitivityChangeRate,period,windowWidth, oooThreshold);
        return result;
    }
}
