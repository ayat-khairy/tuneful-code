package cl.cam.ac.uk.tuneful.wl.characterization;

import java.util.ArrayList;
import java.util.Hashtable;

public class WorkloadMonitor {
    //TODO: change that to be loaded from a config file instead of being hard coded
	private static final String API_URL = "http://bda-1.dtg.cl.cam.ac.uk:18080/api/v1/applications";

	public ExectuionProvenance getWLExecutionProv(String appId) {
		return new ProvenanceCapturer(API_URL).captureAppExecutionProv(appId);
	}

	
	//TODO: fill the code
	public Hashtable<String, String> getWLConf(String appId) {
		// read the conf from SparkConf object ... pass the conf params names
		// fill the table with the returned values
		
		
		
		return null;
	}
	
	//TODO: fill the code
		public Hashtable<String, String> getWLConfOffline(String appId , ArrayList< String> confParams) {
			// read the conf from the history env page ... pass the conf params names
			// fill the table with the returned values
			
			return new ProvenanceCapturer(API_URL).captureAppExecutionConf(appId , confParams);
			
		
		}

	public double getWLExecutionTime(String appId) {
		return new ProvenanceCapturer(API_URL).getWorkloadExecutionCost(API_URL, appId);
	}

	public String getAppId(String appName) {
		new ProvenanceCapturer(API_URL).getLastWLExecutionId (appName);
		
		return null;
	}

	public int countExecutionInstances(String appName) {
		return new ProvenanceCapturer(API_URL).getWLExectionsCount (appName);
	}

	public double getAppExecCost(String finerprintAppId) {

		return getWLExecutionTime(finerprintAppId);
	}

}
