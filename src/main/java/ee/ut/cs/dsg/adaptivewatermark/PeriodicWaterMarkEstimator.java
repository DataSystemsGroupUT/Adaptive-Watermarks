package ee.ut.cs.dsg.adaptivewatermark;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

import moa.stream.ADWIN;

public class PeriodicWaterMarkEstimator extends WatermarkEstimator 
{
	private long maxTimestampSeen;
	private long maxAllowedOutOfOrder;
//	private long watermarkPeriod;
	private boolean watermarkChanged = false;

	private class PeriodicWaterMarkUpdater extends Thread implements Serializable
	{

		private static final long serialVersionUID = -5970954394789028165L;
		private long watermarkPeriod;
		private PeriodicWaterMarkEstimator estimator;
		private volatile boolean running=true;
		
		public PeriodicWaterMarkUpdater(PeriodicWaterMarkEstimator esti, long wp) {

			watermarkPeriod = wp;
			estimator = esti;
		}
		
		public void terminate()
		{
			running = false;
		}
		
		@Override
	    public void run() 
		{
			try {

				while(running)
				{
					sleep(watermarkPeriod);
					//Thread.sleep();
					System.out.println("Invoking estimation");

					estimator.estimateNextWatermark();
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}

	PeriodicWaterMarkUpdater updater;
	public PeriodicWaterMarkEstimator(long max, long wp) {

		maxTimestampSeen = Long.MIN_VALUE;
		maxAllowedOutOfOrder = max;
		updater = new PeriodicWaterMarkUpdater(this, wp);

		updater.start();
		
	}

	public void terminate()
	{
		updater.terminate();
		try {
			updater.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	public PeriodicWaterMarkEstimator(long max, long wp, BufferedWriter wrtr) {

		maxTimestampSeen = Long.MIN_VALUE;
		maxAllowedOutOfOrder = max;
		
	}
	@Override
	public synchronized boolean processEvent(long nextElementTimestamp, long referenceTimestamp)
	{
		//System.out.println("Processing element");
		lastEventArrivedLate = false;
		if (nextElementTimestamp < lastWatermark)
		{
//			lateEventsCount++;
			lastEventArrivedLate=true;
		}
		maxTimestampSeen = Math.max(maxTimestampSeen, nextElementTimestamp);
		System.out.println("Max Time Stamp Seen "+maxTimestampSeen);
		if (watermarkChanged)
		{
			watermarkChanged = false;
			return true;
		}

		return false;
		
	}
	@Override
	public synchronized void  estimateNextWatermark()
	{
		//System.out.println("Estimating watermark... Max Time Stamp Seen "+maxTimestampSeen);
		if (maxTimestampSeen == Long.MIN_VALUE)
		{
			lastWatermark = 0;
			
		}
	//	long tt = lastWatermark == 0? 0: lastWatermark;
		if (maxTimestampSeen == Long.MIN_VALUE)
			return;
		if ((maxTimestampSeen - maxAllowedOutOfOrder) > lastWatermark)
		{
			
			lastWatermark = maxTimestampSeen - maxAllowedOutOfOrder;
			System.out.println("Next watermark "+lastWatermark);
			watermarkChanged = true;
			
		}
		else
		{
			watermarkChanged = false;
		}
		
		
	}
	
}