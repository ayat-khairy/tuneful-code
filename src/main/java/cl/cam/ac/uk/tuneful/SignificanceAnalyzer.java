package cl.cam.ac.uk.tuneful;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;
import cl.cam.ac.uk.tuneful.util.Util;

public class SignificanceAnalyzer {

	List<String> allParamsNames;
	Hashtable<String, List<String>> sigParamsNames;
	Hashtable<String, ConfParam> allparams;
	int n_SA_rounds;
	Hashtable<String, Integer> n_executions;
	int n_samples_per_SA;
	Hashtable<String, Integer> current_SA_round;
	float fraction;
	private String TUNEFUL_HOME;
	private String SA_SCRIPT_FILE;

	private String sigParamsPath;
	private String n_executionsPath;
	private String currentSARoundPath;

	public SignificanceAnalyzer() {
		TUNEFUL_HOME = TunefulFactory.getTunefulHome();
		SA_SCRIPT_FILE = "SA.py";
		n_SA_rounds = 2; // TODO: make configurable
		n_samples_per_SA = 3; // samples per SA round
		current_SA_round = new Hashtable<String, Integer>();
		n_executions = new Hashtable<String, Integer>();// number of WL executions
		sigParamsNames = new Hashtable<String, List<String>>();
		allParamsNames = TunefulFactory.getTunableParams();
		sigParamsPath = TunefulFactory.getTunefulHome() + "/sigparams.ser";
		n_executionsPath = TunefulFactory.getTunefulHome() + "/n_executions.ser";
		currentSARoundPath = TunefulFactory.getTunefulHome() + "/currentSARound.ser";
		sigParamsNames = Util.loadTable(sigParamsPath);
		current_SA_round = Util.loadTable(currentSARoundPath);
		n_executions = Util.loadTable(n_executionsPath);
		allparams = TunefulFactory.getTunableParamsRange();
		fraction = 0.8f; // TODO: make configurable
		copySAScriptToTunefulHome();

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
			TunefulFactory.getConfigurationSampler().resetIndex(appName); // to handle the new
																			// sig params space
			sigParamsNames.put(appName, sigParams);
			n_executions.put(appName, 0); // reset the number of execution for the new SA round
		}
		n_executions.put(appName, n_executions.get(appName) + 1); // increment number of samples
		Util.writeTable(n_executions, n_executionsPath);
		Util.writeTable(current_SA_round, currentSARoundPath);
		Util.writeTable(sigParamsNames, sigParamsPath);

		return TunefulFactory.getConfigurationSampler().sample(appName, sigParamsNames.get(appName));

	}

	private List<String> performSARound(String appName) {
		String SA_PYTHON_SCRIPT = TUNEFUL_HOME + "/" + SA_SCRIPT_FILE;
		System.out.println(">>> " + SA_PYTHON_SCRIPT);

//		final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
//		String python_home = envMap.get("PYTHON_HOME");
//
//		if (python_home == null) {
//			System.err.println("PYTHON_HOME is not set! ...");
//			return null;
//		}

		String samplesFileName = TunefulFactory.getSamplesFilePath(appName, current_SA_round.get(appName) - 1);// perform
																												// SA
																												// for
																												// the
																												// earlier
																												// round
		String sigParamsFullPath = TUNEFUL_HOME + "/"
				+ (String) TunefulFactory.getSigParamsFileName(appName, current_SA_round.get(appName));
		String command = "python " + SA_PYTHON_SCRIPT + " " + samplesFileName + " " + sigParamsNames.get(appName).size()
				+ " " + fraction + " " + sigParamsFullPath;
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

		List<String> readSigParams = readSigParams(sigParamsFullPath);
		System.out.println(">> SA_round >>" + current_SA_round.get(appName) + ">> Sig Params" + readSigParams);
		
		
//		Util.writeTable(sigParamsNames, sigParamsPath);
//		Util.writeTable(current_SA_round, currentSARoundPath);

		return readSigParams;

	}

	private List<String> readSigParams(String sigParamsFilePath) {

		FileReader fr;
		try {
			fr = new FileReader(sigParamsFilePath);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			line = line.substring(0, line.lastIndexOf(','));  // get rid of extra ',' at the end
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
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().mapToNumeric("spark.executor.memory", "66m"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().mapToNumeric("spark.memory.fraction", "0.6"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().mapToNumeric("spark.rdd.compress", "true"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().mapToNumeric("spark.serializer", "org.apache.spark.serializer.KryoSerializer"));
		// testing

//		TunefulFactory.getConfigurationSampler().createNewSampler("test", 5);
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(">>>>" + TunefulFactory.getConfigurationSampler().getNextSample("test").values());
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
//		System.err.println(TunefulFactory.getSignificanceAnalyzer().performSARound("test"));
//		System.out.println(TunefulFactory.getSignificanceAnalyzer().suggestNextConf("test"));
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
			String samplesFileName = TunefulFactory.getSamplesFilePath(appName, current_SA_round.get(appName));
			FileWriter fileWriter = new FileWriter(samplesFileName , true);
			List<String> sigParams = sigParamsNames.get(appName);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			System.out.println(">> storeAppExecution >> n_executions >>" + n_executions.get(appName));
			if (n_executions.get(appName) == 1) /// check if first execution in this SA round
			{
				String header = "";
				//write header
				for (int i = 0; i < sigParams.size(); i++) {
					header += sigParams.get(i) + ",";
				}
				bufferedWriter.write(header + "\n");
			}
			
			String appExec = "";
			for (int i = 0; i < sigParams.size(); i++) {
				appExec += mapToNumeric(sigParams.get(i) , sparkConf.get(sigParams.get(i) )) + ",";
			}
			bufferedWriter.write(appExec + execTime + "\n");
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private String mapToNumeric(String paramName, String value) {
		String numericValue = "";
		if (allparams.get(paramName).getType().equalsIgnoreCase("int")) {
			numericValue = Integer.parseInt(value.replaceAll("\\D*", "")) + "";  // keep the digits only and get rid of any units

		} else if (allparams.get(paramName).getType().equalsIgnoreCase("float")) {
			numericValue = Double.parseDouble(value) + "";
		}

		else if (allparams.get(paramName).getType().equalsIgnoreCase("boolean")) {
			numericValue = value.equals("true") ? 1 + "" : 0 + "";
		} else // enum
		{
			for (int i = 0; i < allparams.get(paramName).getValues().length; i++) {
				if (value.equals(allparams.get(paramName).getValues()[i]))
					return i + "";
			}

		}

		return numericValue;
	}

	private void copySAScriptToTunefulHome() {

		try {

			String jarPath = new File(CostModeler.class.getProtectionDomain().getCodeSource().getLocation().toURI())
					.getPath();
			System.out.println(">>> Jar path >>>" + jarPath);

			Process p;

			String command = "jar xf " + jarPath + " " + SA_SCRIPT_FILE;
			System.out.println(">>>cmd >>> " + command);
			p = Runtime.getRuntime().exec(command, new String[] {}, new File(TUNEFUL_HOME));
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

		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
