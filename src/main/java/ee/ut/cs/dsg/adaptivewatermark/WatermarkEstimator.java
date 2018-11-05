package ee.ut.cs.dsg.adaptivewatermark;

import java.io.Serializable;

public abstract class WatermarkEstimator implements Serializable {

	private static final long serialVersionUID = 341319304641417914L;
	protected long lastWatermark=0;
	public abstract boolean processEvent(long enentTimestamp, long referenceTimestamp);
	public abstract void estimateNextWatermark();
	protected boolean lastEventArrivedLate=false;
	public long getWatermark()
	{
		return lastWatermark;
	}
	public boolean wasLastElementALateArrival()
	{
		return lastEventArrivedLate;
	}


	public long getBufferSize()
	{
		return 0;
	}

        public enum ReferenceEventType
	{
		IngestionTime,
		PreviousEventEventTime
	}
//	public enum WatermarkEstimatorType
//	{
//		DistanceBasedAdaptiveWatermarkEstimator,
//		PessimisticAdaptiveWatermarkEstimator,
//		OptimisiticAdaptiveWatermarkEstimator,
//		PeriodicWatermarkEstimator
//	}
}
