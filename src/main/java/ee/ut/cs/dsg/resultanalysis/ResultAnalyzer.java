package ee.ut.cs.dsg.resultanalysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class ResultAnalyzer {

    public static void main(String[] args) throws Exception {


//        long xxx = 1541732400000L+9223370495122375807L;
//        System.out.println(xxx);
//        if (xxx == Long.MAX_VALUE)
//        {
//
//            System.out.println("You've reached the limit");
//            return;
//        }
//        else
//        {
//            System.out.println("Difference is "+(Long.MAX_VALUE - xxx));
//        }
        String[] files = {"C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.1W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-1.0W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-1.0W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-1.0W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-1.0W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-1.0W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-1.0W-100.txt",
                "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-1.1S-1.0SCR-1.0W-100.txt",
                "C:\\Work\\Data\\DEBSGC2012TSOnlyMillisRecenAdaptiveL-1000OOO-1.1S-1.0SCR-1.0W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-0.01W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-0.01W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.01W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.01W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.01W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-1.1S-1.0SCR-0.01W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-0.1W-1000.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.01S-1.0SCR-0.1W-100.txt",
                "C:\\Work\\Data\\DEBSGC2015TSOnly_part_1_MillisecondAdaptiveL-1000OOO-0.1S-1.0SCR-0.1W-1000.txt"};

    // int k = 51;
        System.out.println(AnalysisResult.getHeader());
        for (String s : files) {
//        for (int i = 0; i <= k; i++) {
            AnalysisResult result = analyzeFile(s);
            System.out.println(result.toString());
        }
    }

    private static AnalysisResult analyzeFile(String file) throws Exception {
        AnalysisResult result = null;

        long numElements = 0;
        long numElementsNotProcessed = 0;
        long numWindows = 0;
        String datasetName;
        long allowedLateness = 0;
        long period = 0;
        double sensitivity = 0F;
        double sensitivityChangeRate = 1.1;
        boolean isAdaptive;
        double oooThreshold = 1.1F;
        long windowWidth = 0;
        double averageLag = 0;
        Map<Long, Long> elementsCountMap = new HashMap();
        Map<Long, Double> windowDelay = new HashMap<>();
        if (file.indexOf("Adaptive") != -1) {
            isAdaptive = true;
            String[] splits = file.split("Adaptive");
            datasetName = splits[0];
            String[] splits2 = splits[1].split("-");
            allowedLateness = Long.valueOf(splits2[1].replace("OOO", ""));
            oooThreshold = Double.valueOf(splits2[2].replace("S", ""));
            sensitivity = Double.valueOf(splits2[3].replace("SCR", ""));
            sensitivityChangeRate = Double.valueOf(splits2[4].replace("W", ""));
            windowWidth = Long.valueOf(splits2[5].replace(".txt", ""));
        } else {
            isAdaptive = false;
            String[] splits = file.split("Periodic");
            datasetName = splits[0];
            String[] splits2 = splits[1].split("-");
            allowedLateness = Long.valueOf(splits2[1].replace("P", ""));
            period = Long.valueOf(splits2[2].replace("W", ""));
            windowWidth = Long.valueOf(splits2[3].replace(".txt", ""));

        }
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        line = reader.readLine();

        while (line != null) {
            String[] content = line.split(",");
            long windowEnd = Long.valueOf(content[1].split("end=")[1].replace("}", ""));
            long lag = Long.valueOf(content[3].replace(")", "").trim());


            if (windowEnd + lag < Long.MAX_VALUE) {
                Long end = new Long(windowEnd);
                if (elementsCountMap.keySet().contains(end))
                //if (windowEnd == lastWindowEnd)
                {
                    long cnt = elementsCountMap.get(end);
                    cnt++;
                    elementsCountMap.put(end,cnt);
                    //numElements++;
                }
                else {
                    elementsCountMap.put(end,Long.valueOf(content[2].trim()));
                    //numElements += Long.valueOf(content[2].trim());
                    numWindows++;
                    windowDelay.put(end,Double.valueOf(content[3].replace(")", "").trim()));
                    //averageLag += Double.valueOf(content[3].replace(")", "").trim());
                }
            } else {
                numElementsNotProcessed += Long.valueOf(content[2].trim());
                //numWindows++;
            }

            line = reader.readLine();
        }
        for (Long w : elementsCountMap.keySet())
        {
            numElements+= elementsCountMap.get(w);
            averageLag += windowDelay.get(w);
        }
        result = new AnalysisResult(datasetName, isAdaptive, numWindows, averageLag / numWindows, numElements
                , allowedLateness, sensitivity, sensitivityChangeRate, period, windowWidth, oooThreshold, numElementsNotProcessed);
        return result;
    }
}
