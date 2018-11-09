package ee.ut.cs.dsg.adaptivewatermark;

//import java.util.Date;

import moa.stream.ADWIN;

@Deprecated
public class DistancebasedAdaptiveWatermarkEstimator extends AdaptiveWatermarkEstimator {

	private long watermarkDistance;
//	private double meanBeforeChange;
	private double meanAfterChange;
//	private double stdDevBeforeChange;
	private boolean withinFirstBuffer = true;
	
	private long minTimestamp = Long.MAX_VALUE;
        private double interArrivalDeviationSum=0;
        private int globalTotalEventCount=0;

	private boolean watermarkDistanceIncreased=false;
	private final long DEFAULT_WATER_MARK_DISTANCE = 1;
	private double distanceChangeRate = 0.1;
	private long watermarkDistanceMax = 1000;
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_RESET = "\u001B[0m";
	public static final String ANSI_GREEN = "\u001B[32m";
	//private boolean 
	public DistancebasedAdaptiveWatermarkEstimator(double pSensitivityChangeRate, double pLateArrivalThreshold, double pDistaneChangeRate, long maxAllowedDistance)
	{
		this(pSensitivityChangeRate, pLateArrivalThreshold);
		distanceChangeRate = pDistaneChangeRate;
		watermarkDistanceMax = maxAllowedDistance;
	}
	
	public DistancebasedAdaptiveWatermarkEstimator(double pSensitivityChangeRate, double pLateArrivalThreshold, double pDistaneChangeRate)
	{
		this(pSensitivityChangeRate, pLateArrivalThreshold);
		distanceChangeRate = pDistaneChangeRate;
	}
	public DistancebasedAdaptiveWatermarkEstimator(double pSensitivityChangeRate, double pLateArrivalThreshold)
	{
		this(1,1,pSensitivityChangeRate, pLateArrivalThreshold);
	}
	public DistancebasedAdaptiveWatermarkEstimator(double sensitivity, long watermarkDistance) {
		super(sensitivity);

		this.watermarkDistance = watermarkDistance;
		
//		parametersNeedToChange = false;
	}
	public DistancebasedAdaptiveWatermarkEstimator(double sensitivity, long watermarkDistance, double sensitivityChangeRate, double lateArrivalThreshold) {
		// TODO Auto-generated constructor stub
		super(sensitivity, sensitivityChangeRate, lateArrivalThreshold);

		this.watermarkDistance = watermarkDistance;
//		parametersNeedToChange = false;
	}
	
	
	@Override
	public void estimateNextWatermark() {
		
		if (withinFirstBuffer)
		{
			lastWatermark = minTimestamp;
			withinFirstBuffer = false;
		}
		else
		{
                    //System.out.println("\t\t\t\tAvg. Interarrival time "+(interArrivalDeviationSum/globalTotalEventCount));
              //        System.out.println("\t\t\t\tAvg. Interarrival time "+(interArrivalDeviationSum/totalEventCount));
			//TODO: Revisit this logic.
			/* *
			 * The idea is to check if the difference is negative. That is the mean after change is smaller than the mean before change. This indicates that deviations are getting less.
			 * that is, events are arriving more regularly. Yet, this does not necessarily mean that these events are not late. It could be the case that they are arriving regular but late.
			 * to judge the lateness, we have to compare the newly arriving event's timestamp to the last watermark emitted. Also to think of a new watermark, we have to take into consideration
			 * the ratio of late arriving events to total processed events within this chunk.
			 * */
			
			
			
			//double lateArrivalRatio = (double)lateEventsCount/totalEventCount;
			//if (maxLateArrivalRatio == 0  && newWatermarkGenerated)
			if (((double)lateEventsCountPerChunk/totalEventCountPerChunk) == 0)//  the average of late arrival ratio
			{
				// Another idea is to increase the watermark distance by the average deviation from the buffer!
				watermarkDistance =  Math.max((long)(watermarkDistance/distanceChangeRate),watermarkDistanceMax);
//				watermarkDistance = (long)Math.ceil(watermarkDistance + Math.abs(meanBeforeChange -  adwin.getEstimation()));
//				watermarkDistance = (long)Math.ceil(watermarkDistance + (meanBeforeChange -  adwin.getEstimation()));
				System.out.println("Watermark distance increased to " +watermarkDistance);
				watermarkDistanceIncreased = true;
			//	changeSensitivity = 1; // a precaution to account for sudden changes in sensitivity
			}
			// General to any adaptive watermark
			
			//
			//if (maxLateArrivalRatio >= lateArrivalThreshold)// && newWatermarkGenerated)
			if (((double)lateEventsCountPerChunk/totalEventCountPerChunk) >= lateArrivalThreshold)// && newWatermarkGenerated)
			{
				if (watermarkDistanceIncreased)
					watermarkDistance = DEFAULT_WATER_MARK_DISTANCE;
				else
					watermarkDistance = Math.max((long)(watermarkDistance*distanceChangeRate),DEFAULT_WATER_MARK_DISTANCE);
				System.out.println("Watermark distance decreased to " +watermarkDistance);
				watermarkDistanceIncreased = false;
				//parametersNeedToChange = false;
			}
			adjustChangeDetectionSensitivity();
			
			//if (maxLateArrivalRatio < lateArrivalThreshold)
			if (((double)lateEventsCountPerChunk/totalEventCountPerChunk) < lateArrivalThreshold)
			{
//				long delta;
//				
//				double difference =  meanAfterChange - meanBeforeChange;
//
//				double deviationRatio = Math.abs(difference)/stdDevBeforeChange;
//
//				//TODO: we can consider the weights in the formula below as parameters
//				double watermarkChangeRatio = deviationRatio;// 0.5 * deviationRatio + 0.5*(1- (lateArrivalRatio));
//
//				delta = (long) Math.floor(watermarkDistance * watermarkChangeRatio) ;
//				
//				if (difference > 0) //deviations are increasing because late arrivals are observed. Next watermark should advance less frequent
//				{
//					// In case deviations are increasing, we should not emit a new watermark?
//					//watermarkDistance = 0;
//					//System.out.println(ANSI_RED + "Increasing in differences" + ANSI_RESET);
//					watermarkDistance = watermarkDistance - delta;
//					
//				}
//				else // deviations are decreasing
//				{
//					//System.out.println(ANSI_GREEN + "Increasing in differences" + ANSI_RESET);
//					watermarkDistance = watermarkDistance + delta;
//				}
//                                watermarkDistance = Math.min(((interArrivalDeviationSum/totalEventCount) > 0 ? (int)(interArrivalDeviationSum/totalEventCount): 0),watermarkDistanceMax);
//                                System.out.println("Actual Increase of watermark is " +watermarkDistance);
				lastWatermark =( watermarkDistance > 0 ? lastWatermark+watermarkDistance : lastWatermark);
                                lateEventsCountPerChunk=0;
                                totalEventCountPerChunk = 0;
                                //maxLateArrivalRatio = 0;
                                sumLateArrivalRatio = 0;
                                interArrivalDeviationSum=0;
				newWatermarkGenerated = true;
                                // new step, reset ADWIN 26-7-2018
                                bufferSize = adwin.getWidth();
                                adwin = new ADWIN(changeSensitivity);
				
			}
			else // we need to change the sensitivity
			{
				newWatermarkGenerated = false;
			}
				 
			
		}
		

	}
	
	@Override
	public boolean processEvent(long eventTimestamp, long referenceTimestamp) 
	{
		// For the first chunk, we need to set the seeding point. Within the first buffer, I choose the oldest timestamp, a bit pessimistic. 
		lastEventArrivedLate = false;
		totalEventCountPerChunk++;
                globalTotalEventCount++;
		if (withinFirstBuffer && eventTimestamp < minTimestamp)
		{
			minTimestamp = eventTimestamp;
		}
		else if (!withinFirstBuffer && eventTimestamp < lastWatermark)
		{
			lateEventsCountPerChunk++;
			lastEventArrivedLate=true;
			double rate = (double)lateEventsCountPerChunk/totalEventCountPerChunk;
			//maxLateArrivalRatio = Math.max(rate, maxLateArrivalRatio);
			sumLateArrivalRatio += rate;
		}
		
		double deviation ;
		deviation = Math.abs(referenceTimestamp - eventTimestamp);
//                deviation = eventTimestamp - referenceTimestamp;
//                System.out.println("Deviation "+deviation);
                interArrivalDeviationSum += (eventTimestamp - referenceTimestamp );
//		System.out.println("Sensitivity "+ changeSensitivity);
                
		if (adwin.setInput(deviation, changeSensitivity))
		{
//			meanAfterChange = adwin.getEstimation();
//                        System.out.println("\t\t\tMean inter arrival time is "+meanAfterChange);
			this.estimateNextWatermark();
			return true;
		}
		else
		{
//			meanBeforeChange = adwin.getEstimation();
//                        System.out.println("Mean so far "+meanBeforeChange);
//			stdDevBeforeChange = Math.sqrt(adwin.getVariance());
			return false;
		}
		
		
	}

}
