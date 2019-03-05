package cl.cam.ac.uk.tuneful.wl.characterization;

import java.util.Hashtable;
import java.util.Set;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class WorkloadCharacterizer {

	public String findSimilarWorkload(String appId, Hashtable<String, String> appsFingerprints) {
		ExectuionProvenance WLExecutionProv = TunefulFactory.getWorkloadMonitor().getWLExecutionProv(appId);
		String similarWL = findNearestNeighbour(WLExecutionProv, appsFingerprints);

		return similarWL;
	}

	private String findNearestNeighbour(ExectuionProvenance prov, Hashtable<String, String> appsFingerprints) {
		Set<String> keys = appsFingerprints.keySet();
		ExectuionProvenance otherAppProv = null ; 
		double minDistance = Double.MAX_VALUE;
		String minDistanceAppName = "";
		for (String key : keys) {
			otherAppProv = TunefulFactory.getWorkloadMonitor().getWLExecutionProv(appsFingerprints.get(key));
			double distance = calculateDistance(prov, otherAppProv);
			if (distance < minDistance) {
				minDistance = distance;
				minDistanceAppName = key;
			}
		}
		return minDistanceAppName;
	}

	public static double calculateDistance(ExectuionProvenance app1, ExectuionProvenance app2) {
		String point1AsStr = app1.asString();
		String point2AsStr = app2.asString();
		System.out.println(">> point1 >> " + point1AsStr);
		System.out.println(">> point2 >> " + point2AsStr);
		String[] point1 = point1AsStr.split(",");
		String[] point2 = point2AsStr.split(",");
		System.out.println(point1.length + " >>>> " + point2.length);
		double Sum = 0.0;
		for (int i = 0; i < point1.length; i++) {
			Sum = Sum + Math.pow((Double.parseDouble(point1[i]) - Double.parseDouble(point2[i])), 2.0);
		}
		return Math.sqrt(Sum);
	}

}
