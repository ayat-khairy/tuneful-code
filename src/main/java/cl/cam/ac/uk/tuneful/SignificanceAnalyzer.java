package cl.cam.ac.uk.tuneful;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class SignificanceAnalyzer {

	List<String> allParamsNames;
	Hashtable<String, List<String>> sigParamsNames;
	Hashtable<String, ConfParam> allparams;
	int n_SA_rounds;
	Hashtable<String, Integer> n_executions;
	int n_samples_per_SA;
	Hashtable<String, Integer> current_SA_round;
	float fraction;

	public SignificanceAnalyzer() {
		n_SA_rounds = 2; // TODO: make configurable
		n_samples_per_SA = 3; // samples per SA round
		current_SA_round = new Hashtable<String, Integer>();
		n_executions = new Hashtable<String, Integer>();// number of WL executions
		allParamsNames = TunefulFactory.getTunableParams();
		allparams = TunefulFactory.getTunableParamsRange();
		sigParamsNames = new Hashtable<String, List<String>>();
		fraction = 0.45f; // TODO: make configurable
	}

	public Hashtable<String, String> suggestNextConf(String appName) {

		if (!sigParamsNames.containsKey(appName)) {// the first time to execute this WL so initialize sig params with
													// all
			System.out.println(">> app did not run before ..."); // the params
			System.out.println("table size >> " + sigParamsNames.size());
			sigParamsNames.put(appName, allParamsNames); // all params are significant initially then shrink after each
															// round
			n_executions.put(appName, new Integer(0)); // init n_executions
			current_SA_round.put(appName, new Integer(0)); // init SA_round
			TunefulFactory.getConfigurationSampler().createNewSampler(appName, allParamsNames.size()); // init sampler
		} else if (n_executions.get(appName) >= n_samples_per_SA) { // should do SA round
			if (!current_SA_round.containsKey(appName))
				current_SA_round.put(appName, new Integer(0));
			else
				current_SA_round.put(appName, new Integer(current_SA_round.get(appName).intValue() + 1));

			List<String> sigParams = performSARound(appName);
			TunefulFactory.getConfigurationSampler().createNewSampler(appName, sigParams.size()); // to handle the new
																									// sig params space
			sigParamsNames.put(appName, sigParams);
			n_executions.put(appName, 0); // reset the number of execution for the new SA round
		}
		n_executions.put(appName, n_executions.get(appName) + 1); // increment number of samples
		return TunefulFactory.getConfigurationSampler().sample(appName, sigParamsNames.get(appName));

	}

	private List<String> performSARound(String appName) {
		String SA_PYTHON_SCRIPT = Thread.currentThread().getContextClassLoader().getResource("SA.py").getPath();
		System.out.println(">>> " + SA_PYTHON_SCRIPT);
		SA_PYTHON_SCRIPT = SA_PYTHON_SCRIPT.substring(1); // get rid of the extra "/" at the begining ... get rid of//
															// this line when testing on linux or mac
		final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
		String python_home = envMap.get("PYTHON_HOME");

		if (python_home == null) {
			System.err.println("PYTHON_HOME is not set! ...");
			return null;
		}

		String samplesFileName = TunefulFactory.getSamplesFileName(appName, current_SA_round.get(appName));
		String sigParamsFileName = (String) TunefulFactory.getSigParamsFileName(appName, current_SA_round.get(appName));
		String command = python_home + "\\python " + SA_PYTHON_SCRIPT + " " + samplesFileName + " "
				+ sigParamsNames.get(appName).size() + " " + fraction + " " + sigParamsFileName;
		Process p;
		try {
			p = Runtime.getRuntime().exec(command);
			p.waitFor();
			BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
			BufferedReader bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line;
			while ((line = bri.readLine()) != null) {
				System.out.println(line);
			}
			bri.close();
			while ((line = bre.readLine()) != null) {
				System.out.println(line);
			}
			bre.close();
			p.waitFor();
			System.out.println("Done.");

			p.destroy();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		List<String> readSigParams = readSigParams(sigParamsFileName);
		System.out.println(">> SA_round >>" + current_SA_round.get(appName) + ">> Sig Params" + readSigParams);
		return readSigParams;

	}

	private List<String> readSigParams(String sigParamsFileName) {

		FileReader fr;
		try {
			fr = new FileReader(
					Thread.currentThread().getContextClassLoader().getResource(sigParamsFileName).getPath());
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			String[] params = line.split(",");
			return Arrays.asList(params);

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static void main(String[] args) {

		// testing

//		TunefulFactory.getConfigurationSampler().createNewSampler("test", 5);
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
//		System.err.println(TunefulFactory.getSignificanceAnalyzer().performSARound("test"));
		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
	}

	public List<String> getSignificantParams(String appName) {
		return sigParamsNames.get(appName);
	}

	public Hashtable<String, ConfParam> getAllParams() {
		return allparams;
	}

	public boolean isSigParamDetected(String appName) {
		if (current_SA_round.get(appName) == null)
			return false;
		return current_SA_round.get(appName).intValue() == 2;
	}

	public void storeAppExecution(SparkConf sparkConf, long execTime, String appName) {
		try {
			String samplesFileName = TunefulFactory.getSamplesFileName(appName, current_SA_round.get(appName));
			FileWriter fileWriter = new FileWriter(samplesFileName);
			List<String> sigParams = sigParamsNames.get(appName);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String appExec = "";
			for (int i = 0; i < sigParams.size(); i++) {
				appExec += sparkConf.get(sigParams.get(i)) + ",";
			}
			bufferedWriter.write(appExec+"\n");
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
