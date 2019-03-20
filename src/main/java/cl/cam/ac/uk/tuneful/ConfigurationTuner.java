package cl.cam.ac.uk.tuneful;

import java.util.Hashtable;

public class ConfigurationTuner {
	private static final int NUMBER_OF_RUNS = 2;
	// TODO: save this data in property file and load in the constructor on start
	Hashtable<String, String> modeledApps = null; // key is the app name and the value is the model path/grid/folder
	Hashtable<String, String> appsFingerprints = null; // key is the app name and the value is the appId that ran under
														// the fingerprint
	// fixed conf
	String appModelPath = ".//costModels";
    private Hashtable<String, String> sigParams;

	public ConfigurationTuner() {
		modeledApps = new Hashtable<String, String>();
		appsFingerprints = new Hashtable<String, String>();
		sigParams = new Hashtable<String,String>();  // map that has appName as a key and comma seperated sig params as value 
	}

//	public SparkConf tuneConf(String appName, SparkConf conf) {
//		SparkConf tunedConf = conf;
//
//		// check if sig parameters identified
//		if (!sigParamsIdentified(appName)) {
//            Hashtable<String, String> suggestedConf = TunefulFactory.getSignificanceAnalyzer().suggestNextConf(appName);
//            //set SparkConf to the suggestedconf
//            
//            
//		} else {
//			// check if app has a cost model
//			if (modeledApps.contains(appName)) {
//
//				tunedConf = TunefulFactory.getCostModeler((String) modeledApps.get(appName)).findCandidateConf(conf);
//
//			} else {
//				// check if we have a finger print for this app
//				if (appsFingerprints.contains(appName)) {
//					// check if there is a similar workload
//					String similarAppName = TunefulFactory.getWorkloadCharacterizer()
//							.findSimilarWorkload((String) appsFingerprints.get(appName), appsFingerprints);
//					if (similarAppName != null) {
//
//						tunedConf = TunefulFactory.getCostModeler((String) modeledApps.get(similarAppName))
//								.findCandidateConf(conf);
//					} else {
//						// if no, then pick random conf
//						tunedConf = pickRandomConf(conf);
//					}
//				} else {
//
//					// if no, return the fingerprint fixed configuration and add appId to the
//					// fingerprint list
//					String appId = conf.get("spark.app.id");
//					tunedConf = getFingerprintingConf(conf);
//
//					appsFingerprints.put(appName, appId);
//				}
//			}
//		}
//
//		return tunedConf;
//	}
//
////	public SparkConf tuneConf(String appName, SparkConf conf) {
////		SparkConf tunedConf = conf;
////		// check if app has a cost model
////		if (modeledApps.contains(appName)) {
////	
////			tunedConf = TunefulFactory.getCostModeler((String) modeledApps.get(appName)).findCandidateConf(conf);
////
////		} else {
////			// check if we have a finger print for this app
////			if (appsFingerprints.contains(appName)) {
////				// check if there is a similar workload
////				String similarAppName = TunefulFactory.getWorkloadCharacterizer()
////						.findSimilarWorkload((String) appsFingerprints.get(appName), appsFingerprints);
////				if (similarAppName != null) {
////
////					tunedConf = TunefulFactory.getCostModeler((String) modeledApps.get(similarAppName))
////							.findCandidateConf();
////				} else {
////					// if no, then pick random conf
////					tunedConf = pickRandomConf(conf);
////				}
////			} else {
////
////				// if no, return the fingerprint fixed configuration and add appId to the
////				// fingerprint list
////				String appId = conf.get("spark.app.id");
////				tunedConf = getFingerprintingConf(conf);
////
////				appsFingerprints.put(appName, appId);
////			}
////		}
////
////		return tunedConf;
////	}
//
//	private boolean sigParamsIdentified(String appName) {
//		// check map of identified params
//		if (sigParams.get(appName) != null)
//			return true;
//		return false;
//	}
//
//	public SparkConf getFingerprintingConf(SparkConf conf) {
//		System.out.println(">> getting the app fingerprint ... ");
//		// TODO: change this to use the most representative conf, now it uses the
//		// default conf
//
//		return conf;
//	}
//
//	private SparkConf pickRandomConf(SparkConf conf) {
//		System.out.println(">> picking Random Conf ... ");
//		// TODO Auto-generated method stub, change to random values within each param
//		// range
//
//		SparkConf randomConf = conf;
//		randomConf.set("spark.executor.cores", new Random().nextInt(7) + "");
//		randomConf.set("spark.executor.memory", new Random().nextInt(28) + "");
//
//		return randomConf;
//	}
//
//	public void updateTuningModel(String appName, SparkConf tunedConf) {
//		System.out.println(">> updating the tuning model ...");
//		String appId = tunedConf.get("spark.app.id");
//		String modelPath = appModelPath + "//" + appName;
//		// check if we have model for this appName
//		if (modeledApps.contains(appName)) {
//			// if yes update the model
//			TunefulFactory.getCostModeler(modelPath).updateModel(appId);
//		} else {
//			// if no, check that we have at least two logged runs for this appName then
//			// build the model and add the app to the modeled apps
//			int executionInstancesCount = TunefulFactory.getWorkloadMonitor().countExecutionInstances(appName);
//			if (executionInstancesCount > NUMBER_OF_RUNS) {
//				TunefulFactory.getCostModeler(modelPath).buildModel(appName, appsFingerprints.get(appName));
//				modeledApps.put(appName, modelPath);
//			}
//		}
//
//	}
//
//	public static void main(String[] args) {
//
//	}

}
