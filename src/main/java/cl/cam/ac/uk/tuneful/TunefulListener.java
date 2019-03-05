package cl.cam.ac.uk.tuneful;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.*;

public class TunefulListener extends SparkListener {
//	 (sparkConf: SparkConf) 
//	override def

	private SparkConf sparkConf;

	public TunefulListener(SparkConf conf) {
		// TODO Auto-generated constructor stub
		this.sparkConf = conf;
	}

	@Override 
	 public void  onApplicationStart( SparkListenerApplicationStart applicationStart) {
			    //println(s"Application ${applicationStart.appId} started at ${applicationStart.time}")
//			    appInfo.applicationID = applicationStart.appId.getOrElse("NA");
//			    appInfo.startTime     = applicationStart.time;
		
		// save the start time of this appName
		// find the conf using tuneful
		
			  }

	@Override 
	 public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
//			    stageMap.map(x => x._2).foreach( x => x.tempTaskTimes.clear());
//			    //println(s"Application ${appInfo.applicationID} ended at ${applicationEnd.time}")
//			    appInfo.endTime = applicationEnd.time;
		
		// find endTime, subtract from the start time
		// store the conf values and exec time in the samples file

	
}
}
