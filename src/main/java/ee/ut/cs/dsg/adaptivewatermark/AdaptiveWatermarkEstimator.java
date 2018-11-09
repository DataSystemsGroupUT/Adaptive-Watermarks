package ee.ut.cs.dsg.adaptivewatermark;

import java.text.SimpleDateFormat;
import java.util.Date;
import moa.stream.ADWIN;

public class AdaptiveWatermarkEstimator extends WatermarkEstimator{

	// We need to know the following anyway
	// Watermark Distance
	// Lastwatermark
	// Mean before Change
	// Mean after change
	// Std Dev before change
	//

	protected final double LATE_ARRIVAL_THRESHOLD = 1.1;
	protected final double SENSITIVITY_CHANGE_RATE = 1.0;
	protected ADWIN adwin;
	protected long lastTimestamp;
	protected double changeSensitivity;
	protected double sensitivityChangeRate;
	protected double lateArrivalThreshold;
	protected long lateEventsCountPerChunk = 0;
	protected long lateEventsGlobal = 0;
	protected long totalEventCountPerChunk =0;
	protected long totalEventCountGlobal =0;
	//	protected double maxLateArrivalRatio = 0;
	protected double sumLateArrivalRatio = 0;
	//	protected boolean parametersNeedToChange;
	protected boolean newWatermarkGenerated = false;
	protected long bufferSize = 0;
	protected long maxAllowedLatness = 0;
	protected long maxDeviationPerChunk=Long.MIN_VALUE;

	protected long maxTimestamp = Long.MIN_VALUE;
	protected long minTimestamp = Long.MAX_VALUE;

	protected long numberOfGeneratedWatermarks=0;
	protected double totalPendingWindows=0;
	protected double maxDeviation=1;

	protected long warmupTupleCount=1000;
	protected boolean continousAdaptationOfLateness=false;
	public AdaptiveWatermarkEstimator()
	{
		adwin = new ADWIN(1);
		changeSensitivity = 1;
		sensitivityChangeRate = SENSITIVITY_CHANGE_RATE;
		lateArrivalThreshold = LATE_ARRIVAL_THRESHOLD;

	}
	public AdaptiveWatermarkEstimator(double senstivity)
	{
		adwin = new ADWIN(senstivity);
		changeSensitivity = senstivity;
		sensitivityChangeRate = SENSITIVITY_CHANGE_RATE;
		lateArrivalThreshold = LATE_ARRIVAL_THRESHOLD;
		maxAllowedLatness = 1000;
	}

	public AdaptiveWatermarkEstimator(double sensitivity, double sensitivityChangeRate, double lateArrivalThreshold)
	{
		adwin = new ADWIN(sensitivity);
		changeSensitivity = sensitivity;
		this.sensitivityChangeRate = sensitivityChangeRate;
		this.lateArrivalThreshold = lateArrivalThreshold;
		maxAllowedLatness=1000;

	}
	public AdaptiveWatermarkEstimator(double sensitivity, double sensitivityChangeRate, double lateArrivalThreshold, long maxLateness)
	{
		this(sensitivity, sensitivityChangeRate, lateArrivalThreshold);
		maxAllowedLatness= maxLateness;
		if (warmupTupleCount > 0)
			maxAllowedLatness=0;
	}
	@Override
	public long getBufferSize()
	{
		return adwin.getWidth()==0? bufferSize: adwin.getWidth();
	}

	protected void adjustChangeDetectionSensitivity()
	{
		//if (maxLateArrivalRatio < lateArrivalThreshold)//  && newWatermarkGenerated) // perfect no out of order for this chunk
		//if ((sumLateArrivalRatio/totalEventCount) < lateArrivalThreshold)//  && newWatermarkGenerated) // perfect no out of order for this chunk
		if (((double)lateEventsCountPerChunk/totalEventCountPerChunk) < lateArrivalThreshold)//  && newWatermarkGenerated) // perfect no out of order for this chunk
		{
			// maybe we can increase sensitivity
			changeSensitivity = (changeSensitivity/sensitivityChangeRate) >= 1? 1:changeSensitivity/sensitivityChangeRate;
			System.out.println("Sensitivity increased to "+changeSensitivity);

			//parametersNeedToChange = false;
		}
		//		else if (maxLateArrivalRatio >= lateArrivalThreshold)// && newWatermarkGenerated)
		else if (((double)lateEventsCountPerChunk/totalEventCountPerChunk) >= lateArrivalThreshold)// && newWatermarkGenerated)
		{
			changeSensitivity = Math.max(changeSensitivity*sensitivityChangeRate, 0.00000000000000000000001);
			System.out.println("Sensitivity decreased to "+changeSensitivity);
			//	parametersNeedToChange = false;
		}
	}

	@Override
	public boolean processEvent(long eventTimestamp, long referenceTimestamp) {
		// For the first chunk, we need to set the seeding point. Within the first buffer, I choose the oldest timestamp, a bit pessimistic.
		totalEventCountGlobal++;
		if (totalEventCountGlobal <= warmupTupleCount)
		{
			maxAllowedLatness = Math.max(maxAllowedLatness, Math.abs(referenceTimestamp - eventTimestamp));
			return false;
		}
		if (totalEventCountGlobal == warmupTupleCount+1) // just finished the warmup
		{
			//maxAllowedLatness = maxTimestamp - minTimestamp;
			System.out.println("Estimated skewness "+maxAllowedLatness);

		}
		totalEventCountPerChunk++;
		lastEventArrivedLate = false;
		if (eventTimestamp < lastWatermark)
		{
			lateEventsCountPerChunk++;
			lastEventArrivedLate=true;
		}

		if (eventTimestamp > maxTimestamp)
		{
			maxTimestamp = eventTimestamp;
		}
		if (eventTimestamp < minTimestamp)
		{
			minTimestamp = eventTimestamp;
		}
		//double rate =(double)lateEventsCountPerChunk/totalEventCount;
		//		maxLateArrivalRatio = Math.max(rate, maxLateArrivalRatio);
		//sumLateArrivalRatio += rate;
		long deviation;
//		deviation = (Math.abs(referenceTimestamp - eventTimestamp))/(24L*60L*60L*1000L);
//		deviation = (eventTimestamp - referenceTimestamp);
		deviation = Math.abs(referenceTimestamp - eventTimestamp);
		maxDeviationPerChunk = Math.max(maxDeviationPerChunk,deviation);
//		System.out.println("Deviation is "+deviation);
		if (adwin.setInput((double)deviation/maxAllowedLatness, changeSensitivity))
		{
			this.estimateNextWatermark();
			//					lateEventsCount=0;
			//					totalEventCount = 0;
			maxTimestamp = Long.MIN_VALUE;
			minTimestamp = Long.MAX_VALUE;
			return true;
		}
		else
		{
			return false;
		}
	}

	@Override
	public void estimateNextWatermark() {

		//double lateArrivalRatio = (double)lateEventsCount/totalEventCount;
		adjustChangeDetectionSensitivity();
		long lateness = maxTimestamp - minTimestamp;
		System.out.println("Max lateness per chunk ="+lateness);
		System.out.println("Elements per chunk "+totalEventCountPerChunk);
		System.out.println("Late arrival ratio "+ ((double)lateEventsCountPerChunk/totalEventCountPerChunk));
		if (lastWatermark == 0 || ((lastWatermark < (minTimestamp - lateness)) && (((double)lateEventsCountPerChunk/totalEventCountPerChunk) < lateArrivalThreshold)))
		{
			//lastWatermark = minTimestamp - lateness;
			lastWatermark = maxTimestamp - maxAllowedLatness;//lateness;
			if (continousAdaptationOfLateness)
			{
				maxAllowedLatness = maxDeviationPerChunk;
				System.out.println("Estimated max skewness changed to "+maxAllowedLatness);
				maxDeviationPerChunk = Long.MIN_VALUE;
			}
			lateEventsGlobal+=lateEventsCountPerChunk;
			lateEventsCountPerChunk=0;
			totalEventCountPerChunk = 0;
			sumLateArrivalRatio = 0;

			bufferSize = adwin.getWidth();
//			adwin = new ADWIN(changeSensitivity);
		}
		else
		{
			SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			Date d = new Date(maxTimestamp-lateness);
			System.out.println("Prospective time stamp "+sdfDate.format(d));
		}

	}
}
