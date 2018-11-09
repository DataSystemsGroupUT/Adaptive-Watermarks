package ee.ut.cs.dsg.resultanalysis;

public class AnalysisResult {

    private boolean isAdaptive;
    private long numberOfWindows;
    private double averageLagWindowEndWatermark;
    private long numberOfStreamElementsConsidered;
    private long numberOfStreamElementsNotConsidered;
    private long paramAllowedLateness;
    private double paramSensitivity;
    private double paramSensitivityChangeRate;
    private long paramPeriodic;
    private long paramWindoWidth;
    private double paramOOOThreshold;
    private String inputFileName;

    public AnalysisResult(String inputFile, boolean isAdaptive, long numberOfWindows, double averageLagWindowEndWatermark
    , long numberOfStreamElementsConsidered, long pAllowedLateness, double pSensitivity, double pSensitivityChangeRate, long pPeriodic, long pWindowWidth, double pOOOThreshold, long numberOfStreamElementsNotConsidered)
    {
        this.inputFileName = inputFile;
        this.isAdaptive = isAdaptive;
        this.numberOfWindows = numberOfWindows;
        this.averageLagWindowEndWatermark = averageLagWindowEndWatermark;
        this.paramAllowedLateness = pAllowedLateness;
        this.paramSensitivity = pSensitivity;
        this.paramSensitivityChangeRate = pSensitivityChangeRate;
        this.paramPeriodic = pPeriodic;
        this.paramWindoWidth = pWindowWidth;
        this.numberOfStreamElementsConsidered = numberOfStreamElementsConsidered;
        this.numberOfStreamElementsNotConsidered = numberOfStreamElementsNotConsidered;
        this.paramOOOThreshold = pOOOThreshold;
    }

    public boolean isAdaptive(){
        return isAdaptive;
    }
    public long getNumberOfWindows(){return numberOfWindows;
    }

    public double getAverageLagWindowEndWatermark() {
        return averageLagWindowEndWatermark;
    }

    public long getParamAllowedLateness() {
        return paramAllowedLateness;
    }

    public double getParamSensitivity() {
        return paramSensitivity;
    }

    public double getParamSensitivityChangeRate() {
        return paramSensitivityChangeRate;
    }

    public long getParamPeriodic() {
        return paramPeriodic;
    }

    public long getParamWindoWidth() {
        return paramWindoWidth;
    }

    public String getInputFileName() {
        return inputFileName;
    }

    public static String getHeader()
    {
        return "DataSet,Approach,Allowed Lateness, Sensitivity, Sensitivity Change Rate, OOO Threshold, Period, Window Width, Number Of Windows, Number of Elements, Average Lag, Elements Not Processed";
    }
    @Override
    public String toString()
    {
        // data set, adaptive?, allowed lateness, sensitivity, sensitivity rate, ooo threshold, Period, window width, number of windows, number of elements, average watermark lag
        return inputFileName+","+(isAdaptive? "Adaptive":"Periodic")+","+paramAllowedLateness+","+paramSensitivity+","+paramSensitivityChangeRate
                +","+paramOOOThreshold+","+paramPeriodic+","+paramWindoWidth
                +","+numberOfWindows+","+numberOfStreamElementsConsidered
                +","+averageLagWindowEndWatermark+","+numberOfStreamElementsNotConsidered;
    }
}
