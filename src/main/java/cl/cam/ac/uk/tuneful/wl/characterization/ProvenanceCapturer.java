package cl.cam.ac.uk.tuneful.wl.characterization;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class ProvenanceCapturer {
	private String RESTServerIP;
	private String historyServerIP = "http://bda-1.dtg.cl.cam.ac.uk:18080/history/";

	public ProvenanceCapturer(String serverIp) {
		this.setRESTServerIP(serverIp);
	}

//	public void captureTaskExecutionProv(String serverIP, String taskId, int samplingRate) {
//		// use the rest API to get the needed data
//		String response = TunefulFactory.getHttpConnector().connect(serverIP);
//
//		// parse the json response and store data in taskProv object
//		TaskExectuionProvenance taskExecutionProvenance = parseAsTaskExecutionProvenance(response);
//
//		// store the object persistently for later analysis
//
//	}
//
//	private TaskExectuionProvenance parseAsTaskExecutionProvenance(String response) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	private StageExectuionProvenance parseAsStageExecutionProvenance(JSONObject appJson) {

		StageExectuionProvenance stageExecProv = new StageExectuionProvenance();
		stageExecProv.setAppId(appJson.getInt("stageId"));
		stageExecProv.setExecutorRunTime(appJson.getLong("executorRunTime"));
		stageExecProv.setExecutorCpuTime(appJson.getLong("executorCpuTime"));
		stageExecProv.setInputBytes(appJson.getLong("inputBytes"));
		stageExecProv.setInputRecords(appJson.getInt("inputRecords"));
		stageExecProv.setOutputBytes(appJson.getLong("outputBytes"));
		stageExecProv.setOutputRecords(appJson.getInt("outputRecords"));
		stageExecProv.setShuffleReadBytes(appJson.getLong("shuffleReadBytes"));
		stageExecProv.setShuffleReadRecords(appJson.getInt("shuffleReadRecords"));
		stageExecProv.setShuffleWriteBytes(appJson.getLong("shuffleWriteBytes"));
		stageExecProv.setShuffleWriteRecords(appJson.getInt("shuffleWriteRecords"));
		stageExecProv.setMemoryBytesSpilled(appJson.getLong("memoryBytesSpilled"));
		stageExecProv.setDiskBytesSpilled(appJson.getLong("diskBytesSpilled"));

		JSONArray metricsArray = appJson.getJSONArray("accumulatorUpdates");
		for (int i = 0; i < metricsArray.length(); i++) {
			if (metricsArray.getJSONObject(i).toString().contains("resultSize")) {
				stageExecProv.setResultSize(metricsArray.getJSONObject(i).getLong("value"));
			} else if (metricsArray.getJSONObject(i).toString().contains("executorDeserializeCpuTime"))
				stageExecProv.setExecutorDeserializeCpuTime(metricsArray.getJSONObject(i).getLong("value"));
			else if (metricsArray.getJSONObject(i).toString().contains("executorDeserializeTime"))
				stageExecProv.setExecutorDeserializeTime(metricsArray.getJSONObject(i).getLong("value"));
			else if (metricsArray.getJSONObject(i).toString().contains("jvmGCTime"))
				stageExecProv.setGCTime(metricsArray.getJSONObject(i).getLong("value"));

		}
		// System.out.println(stageExecProv.getGCTime());
		// appExecProv.setExecutorDeserializeCpuTime(metricsObject.getLong("executorDeserializeCpuTime"));
		// appExecProv.setExecutorDeserializeTime(metricsObject.getLong("executorDeserializeTime"));
		// appExecProv.setResultSize(metricsArray.getJSONObject(4).get("resultSize"));

		// number of stages

		return stageExecProv;
	}

	public void captureWorkloadProvenanace(String appIdFile, String pathTosave) {
		FileReader fr;
		try {
			fr = new FileReader(appIdFile);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			line = br.readLine(); // to skip the header
			PrintWriter writer = new PrintWriter(pathTosave, "UTF-8");
			ExectuionProvenance appExectuionProvenance = null;
			while (line != null) {
				String[] fileIds = line.split(",");
				for (int i = 0; i < fileIds.length; i++) {
					if (!fileIds[i].equals("")) {
						appExectuionProvenance = captureAppExecutionProv(fileIds[i]);
						// save the prov
						String asString = appExectuionProvenance.asString();
						asString += "," + fileIds[i];
						System.out.println(asString);
						writer.println(asString);

					}
				}
				line = br.readLine();
			}
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void captureWorkloadConfCost(String appIdFile, String pathTosave) {
		FileReader fr;
		try {
			fr = new FileReader(appIdFile);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			line = br.readLine(); // to skip the header
			PrintWriter writer = new PrintWriter(pathTosave, "UTF-8");
			Hashtable<String, String> conf = new Hashtable<String, String>();
			double cost = 0;
			while (line != null) {
				String[] fileIds = line.split(",");
				String asString = "";
				for (int i = 0; i < fileIds.length; i++) {
					if (!fileIds[i].equals("")) {
						conf = captureAppExecutionConf(fileIds[i] , TunefulFactory.getTunableParams());
						cost = getWorkloadExecutionCost(RESTServerIP, fileIds[i]);
						// save the conf and cost
						for (int j = 0; j < conf.size(); j++) {
							asString += conf.get(TunefulFactory.getTunableParams().get(j)) + ",";	
						}
						asString +=  cost;
						System.out.println(asString);
						writer.println(asString);

					}
				}
				line = br.readLine();
			}
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public ExectuionProvenance captureAppExecutionProv(String appId) {
		String stagesUrl = RESTServerIP + "/" + appId + "/stages?status=complete";
		System.out.println(stagesUrl);

		ArrayList<StageExectuionProvenance> stages = getAppStages(stagesUrl);

		ExectuionProvenance appExecutionProvenance = calcuateProvenanceMetrics(stages, getExecutorMemorySize(appId));
		appExecutionProvenance.setConfMemoryPerCore(extractMemoryToCPUConf(appId));
		appExecutionProvenance.setDuration(getWorkloadExecutionCost(RESTServerIP, appId));
		appExecutionProvenance.setName(getAppName(appId));

		// store the object persistently for later analysis
		return appExecutionProvenance;

	}

	public long getWorkloadExecutionCost(String serverIp, String appId) {
		// get the execution time as a cost for now
		String url = serverIp + "/" + appId;
		String response = TunefulFactory.getHttpConnector().connect(url);
		JSONObject appJson = new JSONObject(response);
		JSONArray array = appJson.getJSONArray("attempts");
		long duration = array.getJSONObject(0).getLong("duration");

		return duration;

	}

	private ExectuionProvenance calcuateProvenanceMetrics(ArrayList<StageExectuionProvenance> stages,
			double executorMemory) {
		// loop on the stages and calculate avg metrics and ratios
		ExectuionProvenance appProvenance = new ExectuionProvenance();
		StageExectuionProvenance currentStage;
		double avgExecutorRunTime = 0;
		double avgexecutorCpuTimeToMemoryRatio = 0;
		double avgInOutRatio = 0.0, avgCpuTime = 0.0;
		double avgShuffleReadBytes = 0;
		double avgShuffleWriteBytes = 0;
		double avgMemoryBytesSpilled = 0;
		double avgDiskBytesSpilled = 0;
		double avgExecutorDeserializeToCpuTimeRatio;
		double avgResultSize = 0, avgInputSize;
		double avgGCTime = 0;
		long inputSize = 0, outputSize = 0;
		long duration = 0;
		long deserilaizationCpuTime = 0, deserializationTime = 0, cpuTime = 0;
		int numberOfStages = stages.size();
		for (int i = 0; i < stages.size(); i++) {
			currentStage = stages.get(i);
			avgExecutorRunTime += currentStage.getExecutorRunTime();
			cpuTime += currentStage.getExecutorCpuTime();
			avgexecutorCpuTimeToMemoryRatio += currentStage.getExecutorCpuTime() / executorMemory;
			inputSize += currentStage.getInputBytes();
			outputSize += currentStage.getOutputBytes();
			avgShuffleReadBytes += currentStage.getShuffleReadBytes();
			avgShuffleWriteBytes += currentStage.getShuffleWriteBytes();
			avgMemoryBytesSpilled += currentStage.getMemoryBytesSpilled();
			avgDiskBytesSpilled += currentStage.getDiskBytesSpilled();
			deserializationTime += currentStage.getExecutorDeserializeTime();
			deserilaizationCpuTime += currentStage.getExecutorDeserializeCpuTime();
			avgResultSize += currentStage.getResultSize();
			avgGCTime += currentStage.getGCTime();
		}
		avgCpuTime = cpuTime / numberOfStages;
		avgExecutorRunTime /= numberOfStages;
		avgResultSize /= numberOfStages;
		avgexecutorCpuTimeToMemoryRatio /= numberOfStages;
		avgInputSize = inputSize / numberOfStages;
		avgInOutRatio = avgInputSize / avgResultSize;
		avgShuffleReadBytes /= numberOfStages;
		avgShuffleWriteBytes /= numberOfStages;
		avgMemoryBytesSpilled /= numberOfStages;
		avgDiskBytesSpilled /= numberOfStages;
		avgExecutorDeserializeToCpuTimeRatio = (deserilaizationCpuTime / cpuTime) / numberOfStages;
		avgGCTime /= numberOfStages;

		appProvenance.setAvgExecutorRunTime(avgExecutorRunTime);
		appProvenance.setAvgexecutorCpuTimeToMemoryRatio(avgexecutorCpuTimeToMemoryRatio);
		appProvenance.setAvgInOutRatio(avgInOutRatio);
		appProvenance.setAvgExecutorDeserializeCpuTimeRatio(avgExecutorDeserializeToCpuTimeRatio);
		appProvenance.setAvgShuffleReadBytes(avgShuffleReadBytes);
		appProvenance.setAvgShuffleWriteBytes(avgShuffleWriteBytes);
		appProvenance.setAvgMemoryBytesSpilled(avgMemoryBytesSpilled);
		appProvenance.setAvgDiskBytesSpilled(avgDiskBytesSpilled);
		appProvenance.setAvgExecutorDeserializeCpuTimeRatio(avgExecutorDeserializeToCpuTimeRatio);
		appProvenance.setAvgResultSize(avgResultSize);
		appProvenance.setAvgGCTime(avgGCTime / avgCpuTime); // ratio between GC and CPU time
		appProvenance.setNumberOfStages(numberOfStages);

		return appProvenance;
	}

	private double getExecutorMemorySize(String appId) {
		try {

			String response = TunefulFactory.getHttpConnector().connect(getHistoryServerIP() + appId + "/environment/");
			// String substring =
			// response.substring(response.indexOf("executor.cores=") +
			// ("executor.cores=").length(),
			// response.lastIndexOf("g "));
			// System.out.println(substring);
			String[] substrings = response.split("--conf");
			String coresConf = "";
			String memoryConf = "";
			for (int i = 0; i < substrings.length; i++) {
				if (substrings[i].contains("executor.memory")) {
					memoryConf = substrings[i].substring(
							substrings[i].indexOf("executor.memory=") + "executor.memory=".length(),
							substrings[i].indexOf("g"));
					System.out.println(memoryConf);

				}
			}
			return Double.parseDouble(memoryConf);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	private ArrayList<StageExectuionProvenance> getAppStages(String stagesUrl) {
		// use the rest API to get the needed data
		ArrayList<StageExectuionProvenance> appProvenance = new ArrayList<StageExectuionProvenance>();
		String response = TunefulFactory.getHttpConnector().connect(stagesUrl);
		JSONArray array = new JSONArray(response);
		for (int i = 0; i < array.length(); i++) {
			JSONObject appJson = array.getJSONObject(i);

			appProvenance.add(parseAsStageExecutionProvenance(appJson));
		}

		// parse the json response and store data in taskProv object

		return appProvenance;
	}

	public static void main(String[] args) {
		ProvenanceCapturer provenanceCapturer = new ProvenanceCapturer(
				"http://bda-1.dtg.cl.cam.ac.uk:18080/api/v1/applications");
////		 provenanceCapturer.captureWorkloadProvenanace("confTune_apps.csv",
////		 "workloads-exec-prov-with-appId.txt");
//		 
	//###	provenanceCapturer.captureWorkloadProvenanace("kmeans_wordbags_appids.csv", "kmeans_exec_prov.txt");

//			 provenanceCapturer.captureWorkloadProvenanace("fixed_conf_app_Id.csv",
//			 "fixed_conf.txt");
//		System.out.println(provenanceCapturer.extractCoresConf("app-20180112172833-0080"));
//	System.out.println(provenanceCapturer.extractMemoryConf("app-20180130125955-0093"));

		
		//TODO: create the appID file and fill with workload app Ids
		// read the appIds from a file , for each app Id write the conf and cost to a
		// file
		
//		System.out.println(provenanceCapturer.getConf("app-20180705134519-0403", "spark.executor.extraJavaOptions"));
		provenanceCapturer.captureWorkloadConfCost("appId_extra_conf_kmeans.csv", "extra_conf_cost_kmeans.csv");
		
	}

	private double extractMemoryToCPUConf(String appId) {
		try {
			return (extractMemoryConf(appId) * 1.0) / (extractCoresConf(appId) * 1.0); // cast
																						// to
																						// double
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	}

	public int extractMemoryConf(String appId) {
		try {
			String response = TunefulFactory.getHttpConnector().connect(historyServerIP + appId + "/environment/");
			response = response.substring(response.indexOf("sun.java.command"));
			String[] substrings = response.split("--conf");
			String memoryConf = "";
			for (int i = 0; i < substrings.length; i++) {
				System.out.println(">>>>>>>>>" + substrings[i]);
				if (substrings[i].contains("executor.memory")) {
					memoryConf = substrings[i].substring(
							substrings[i].indexOf("executor.memory=") + "executor.memory=".length(),
							substrings[i].indexOf("g"));
					System.out.println("********** " + memoryConf);
					break;

				}
			}
			return Integer.parseInt(memoryConf);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	}

	public int extractCoresConf(String appId) {
		try {
			String response = TunefulFactory.getHttpConnector().connect(historyServerIP + appId + "/environment/");

			response = response.substring(response.indexOf("sun.java.command"));
			String[] substrings = response.split("--conf");

			String coresConf = "";
			for (int i = 0; i < substrings.length; i++) {
				System.out.println(substrings[i]);
				if (substrings[i].contains("executor.cores")) {
					String substr = substrings[i].substring(substrings[i].indexOf("executor"));
					coresConf = substr.substring(substr.indexOf(".cores=") + ".cores=".length(), substr.indexOf(" "));
					coresConf = coresConf.trim();
					System.out.println(coresConf);
					break;
				}
			}
			return Integer.parseInt(coresConf);
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	}

	public String extractConfValue(String appId, String confName) {
		try {
			String response = TunefulFactory.getHttpConnector().connect(historyServerIP + appId + "/environment/");

			response = response.substring(response.indexOf("sun.java.command"));
			String[] substrings = response.split("--conf");

			String confValue = "";
			for (int i = 0; i < substrings.length; i++) {
				System.out.println(substrings[i]);
				if (substrings[i].contains(confName)) {
					String substr = substrings[i].substring(substrings[i].indexOf(confName));
					confValue = substr.substring(confName.length()+1, substr.indexOf(" ")); // add one to the starting index to get rid of the =
					confValue = confValue.trim();
					System.out.println(confValue);
					break;
				}
			}
			return confValue;
		} catch (Exception e) {
			e.printStackTrace();
			return "-1";
		}

	}

	public String getAppName(String appId) {
		String url = RESTServerIP + "/" + appId;
		String response = TunefulFactory.getHttpConnector().connect(url);
		JSONObject appJson = new JSONObject(response);
		String appName = appJson.getString("name");

		return appName;

	}

	public String getRESTServerIP() {
		return RESTServerIP;
	}

	public void setRESTServerIP(String serverIP) {
		this.RESTServerIP = serverIP;
	}

	private String getHistoryServerIP() {
		return historyServerIP;
	}

	private void setHistoryServerIP(String historyServerIP) {
		this.historyServerIP = historyServerIP;
	}

	public int getWLExectionsCount(String appName) {

		String response = TunefulFactory.getHttpConnector().connect(RESTServerIP);
		JSONArray apps = new JSONArray(response);
		int count = StringUtils.countMatches(apps.toString(), appName);

		return count;
	}

	public String getLastWLExecutionId(String appName) {
		String appId = "";
		String response = TunefulFactory.getHttpConnector().connect(RESTServerIP);
		JSONArray apps = new JSONArray(response);
		for (int i = apps.length() - 1; i > 0; i--) {
			JSONObject obj = apps.getJSONObject(i);
			if (obj.getString("name").equals(appName)) {
				appId = obj.getString("id");
			}
		}
		return appId;

	}

	public Hashtable<String, String> captureAppExecutionConf(String appId, List<String> list) {
		Hashtable<String, String> appConf = new Hashtable<String, String>();
		for (int i = 0; i < list.size(); i++) {
			appConf.put(list.get(i), getConf(appId, list.get(i)));
		}

		return appConf;
	}

	private String getConf(String appId, String confName) {
		String value = "-1";
		// parse the conf value of the passed confParam and return
		if (confName.equalsIgnoreCase("spark.executor.memory")) {
			return getExecutorMemorySize(appId) + "";
		}
		if (confName.equalsIgnoreCase("spark.executor.cores")) {
			return extractCoresConf(appId) + "";
		} else {
			value = extractConfValue(appId, confName);
		}
		return value;
	}

}
