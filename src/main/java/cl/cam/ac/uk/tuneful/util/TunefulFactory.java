package cl.cam.ac.uk.tuneful.util;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import cl.cam.ac.uk.tuneful.ConfParam;
import cl.cam.ac.uk.tuneful.ConfigurationSampler;
import cl.cam.ac.uk.tuneful.ConfigurationTuner;
import cl.cam.ac.uk.tuneful.CostModeler;
import cl.cam.ac.uk.tuneful.SignificanceAnalyzer;
import cl.cam.ac.uk.tuneful.wl.characterization.WorkloadCharacterizer;
import cl.cam.ac.uk.tuneful.wl.characterization.WorkloadMonitor;

public class TunefulFactory {
	static ConfigurationTuner tuner = null;
	static WorkloadCharacterizer characterizer = null;
	static WorkloadMonitor monitor = null;
	static ConfigurationSampler configurationSampler = null;
	static SignificanceAnalyzer significanceAnalyzer = null;
	static HttpConnector connector = new HttpConnector();

	public static HttpConnector getHttpConnector() {
		return connector;
	}

	public static ConfigurationTuner getConfigurationTuner() {
		if (tuner == null) {
			tuner = new ConfigurationTuner();

		}
		return tuner;
	}

	public static WorkloadCharacterizer getWorkloadCharacterizer() {
		if (characterizer == null) {
			characterizer = new WorkloadCharacterizer();

		}
		return characterizer;
	}

	public static WorkloadMonitor getWorkloadMonitor() {
		if (monitor == null) {
			monitor = new WorkloadMonitor();

		}
		return monitor;
	}

	public static CostModeler getCostModeler() {
		return new CostModeler();
	}

	public static List<String> getTunableParams() {
		// TODO read conf params from properties file, have default tunable conf

		String[] conf = { "spark.executor.memory", "spark.rdd.compress", "spark.executor.cores",
				"spark.memory.fraction", "spark.serializer" };
		return Arrays.asList(conf);
	}

	public static Hashtable<String, ConfParam> getTunableParamsRange() {

		// TODO read conf params from properties file, have defaulttunable params ranges
		Hashtable<String, ConfParam> paramsRange = new Hashtable<String, ConfParam>();
		paramsRange.put("spark.executor.memory",
				new ConfParam("spark.executor.memory", "int", new double[] { 3.0, 30.0 }, new String[] { "" }, "g"));
		paramsRange.put("spark.executor.cores",
				new ConfParam("spark.executor.cores", "int", new double[] { 1.0, 7.0 }, new String[] { "" }, ""));
		paramsRange.put("spark.memory.fraction",
				new ConfParam("spark.memory.fraction", "float", new double[] { 0.5, 1 }, new String[] { "" }, ""));
		paramsRange.put("spark.rdd.compress", new ConfParam("spark.rdd.compress", "boolean", new double[] { 0.0, 0.0 },
				new String[] { "true", "false" }, ""));
		paramsRange.put("spark.serializer",
				new ConfParam("spark.serializer", "enum", new double[] { 0.0, 0.0 }, new String[] {
						"org.apache.spark.serializerJavaSerializer", "org.apache.spark.serializer.KryoSerializer" },
						""));

		return paramsRange;

	}

	public static ConfigurationSampler getConfigurationSampler() {
		if (configurationSampler == null) {
			configurationSampler = new ConfigurationSampler();

		}
		return configurationSampler;
	}

	public static SignificanceAnalyzer getSignificanceAnalyzer() {
		if (significanceAnalyzer == null) {
			significanceAnalyzer = new SignificanceAnalyzer();
			System.out.println("SIg Analyzer created ...");

		}
		return significanceAnalyzer;

	}

	public static String getSamplesFileName(String appName, Integer integer) {

		final String BASE = "execution_samples_";
		return BASE + appName + "_SA_" + integer;
	}

	public static Object getSigParamsFileName(String appName, Integer integer) {
		final String BASE = "sig_params_";
		return BASE + appName + "_SA_" + integer;
	}

	public static String getTunefulHome() {
		// TODO read from env var or use default value
		return "/home/ayat/ayat/tuneful";
	}

}
