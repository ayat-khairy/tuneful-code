package cl.cam.ac.uk.tuneful;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class CostModeler {

	private String modelPath = null;
	private String MODEL_INPUT_PATH = "";
	private String BASE_PATH = "";
	private String[] confParams;

	public CostModeler(String modelPath) {
		try {
			loadConfParams();
			BASE_PATH = "\\home\\tuneful\\models";
			this.setModelPath(BASE_PATH + "\\" + modelPath);
			File directory = new File(modelPath);
			if (!directory.exists()) {
				directory.mkdirs();
			}
			MODEL_INPUT_PATH = this.getClass().getClassLoader()
					.getResource("org\\apache\\spark\\spearmint-lite\\tuneful").toURI().getPath() + "\\input";
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void loadConfParams() {
		// TODO: add the remaining params
		confParams = new String[] { "spark.executor.cores", "spark.executor.memory" };
	}

	public CostModeler() {
		// TODO Auto-generated constructor stub
	}

	public SparkConf findCandidateConf(SparkConf conf) {
		Hashtable<String , String> tunedConf = readPendingConf();
		SparkConf updatedConf = conf;
		Set<String> keys = tunedConf.keySet();
		for (String key : keys) {
			updatedConf.set(key, tunedConf.get(key));
		}

		return updatedConf;
	}

	public void buildModel(String appName, String fingerprintAppId) {
		// get the cost and conf of fingerprintAppId
		SparkConf fingerPrintconf = TunefulFactory.getConfigurationTuner().getFingerprintingConf(new SparkConf());
		Hashtable fingerPrintTunedconf = getTunableParams(fingerPrintconf);
		String finerprintAppId = (String) fingerPrintTunedconf.get("spark.app.id");
		double fingerprintCost = TunefulFactory.getWorkloadMonitor().getAppExecCost(finerprintAppId);
		String appId = TunefulFactory.getWorkloadMonitor().getAppId(appName);
		Hashtable conf = TunefulFactory.getWorkloadMonitor().getWLConf(appName);
		double appCost = TunefulFactory.getWorkloadMonitor().getAppExecCost((String) conf.get("spark.app.id"));

		// get the cost and conf of any other random conf and write to a file
		writeToModelInput(fingerPrintTunedconf, fingerprintCost);
		writeToModelInput(conf, appCost);

		// build the cost model in the model path
		runModelBuildCommand();

	}

	private void writeToModelInput(Hashtable fingerPrintTunedconf, double fingerprintCost) {
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(MODEL_INPUT_PATH);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String line = fingerprintCost + " 0 " + getConfAsstr(fingerPrintTunedconf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String getConfAsstr(Hashtable conf, String[] confParams) {
		String confAsStr = "";
		for (int i = 0; i < confParams.length; i++) {
			confAsStr += conf.get(confParams[i]);
		}
		return confAsStr;
	}

	private void writePendingConf(Hashtable fingerPrintTunedconf) {
		FileWriter fileWriter;
		try {
			fileWriter = new FileWriter(MODEL_INPUT_PATH);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			String line = "P P " + getConfAsstr(fingerPrintTunedconf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Hashtable readPendingConf() {
		Hashtable <String, String>conf = new Hashtable<String , String>();
		try {
			FileReader fileReader = new FileReader(MODEL_INPUT_PATH);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			line = bufferedReader.readLine();
			String[] splittedLine = line.split(" ");
			while (line != null) {
				if (splittedLine.length == confParams.length + 2) // number of elements per line
				{
					if (line.contains("P")) {
						// parse line into conf and add to the hashtable
						conf = parseLine(line);

					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return conf;
	}

	private Hashtable<String , String> parseLine(String line) {

		Hashtable <String,String> conf = new Hashtable<String, String>();
		String[] splittedLine = line.split(" ");
		for (int i = 0; i < confParams.length; i++) {
			conf.put(confParams[i], splittedLine[i + 2]);
		}
		return conf;
	}

	private Hashtable<String,String> getTunableParams(SparkConf fingerPrintconf) {
		Hashtable<String, String> tunableParams = new Hashtable<String, String>();
		for (int i = 0; i < confParams.length; i++) {
			tunableParams.put(confParams[i], fingerPrintconf.get(confParams[i]));
		}
		return tunableParams;
	}

	public void updateModel(String appId) {
		/// get the execution instance and update the model with
		Hashtable<String, String> conf = TunefulFactory.getWorkloadMonitor().getWLConf(appId);
		double cost = TunefulFactory.getWorkloadMonitor().getAppExecCost(appId);
		writeToModelInput(conf, cost);
		runModelBuildCommand();

	}

	public String getModelPath() {
		return modelPath;
	}

	public void setModelPath(String modelPath) {
		this.modelPath = modelPath;
	}

	private void runModelBuildCommand() {

		try {

			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHON_HOME");
			File file = new File(new CostModeler().getClass().getClassLoader()
					.getResource("org\\apache\\spark\\spearmint-lite\\spearmint-lite.py").toURI().getPath());

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + pythonFile);

			ProcessBuilder pb = new ProcessBuilder(pythonHome + "\\python ", pythonFile, "--driver=local",
					"--method=GPEIOptChooser", "-method-args=noiseless=1");
			pb.redirectError();
			Process p = pb.start();

			InputStream is = null;
			try {
				is = p.getInputStream();
				int in = -1;
				while ((in = is.read()) != -1) {
					System.out.print((char) in);
				}
			} finally {
				try {
					is.close();
				} catch (Exception e) {
				}
			}

			System.out.println("command executed ! ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		try {

			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHON_HOME");
			File file = new File(new CostModeler().getClass().getClassLoader()
					.getResource("spearmint-lite\\spearmint-lite.py").toURI().getPath());
//			File file = new File (new CostModeler().getClass().getClassLoader()
//					.getResource("test.py").toURI()
//					.getPath());

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + pythonFile);
//			Runtime.getRuntime().exec(new String[] {pythonHome+"\\python " , pythonFile } );

			ProcessBuilder pb = new ProcessBuilder(pythonHome + "\\python ", pythonFile);
			pb.redirectError();
			Process p = pb.start();

			InputStream is = null;
			try {
				is = p.getInputStream();
				int in = -1;
				while ((in = is.read()) != -1) {
					System.out.print((char) in);
				}
			} finally {
				try {
					is.close();
				} catch (Exception e) {
				}
			}

			System.out.println("command executed ! ");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
