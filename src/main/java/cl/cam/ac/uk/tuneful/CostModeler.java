package cl.cam.ac.uk.tuneful;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class CostModeler {

	private String modelPath = null;
	private String MODEL_INPUT_PATH_BASE;

	public CostModeler() {
		MODEL_INPUT_PATH_BASE = "\\home\\tuneful\\spearmint-lite\\";
		File directory = new File(MODEL_INPUT_PATH_BASE);
		if (!directory.exists()) {
			directory.mkdirs();
		}

	}

	// write the config Json file for Spearmint to start the GP optimisation
	private void writeParamsJsonFile(String appName) {
		// write each param name, type, min, max in the app dir
		String filePath = MODEL_INPUT_PATH_BASE + appName + "\\config.json";
		List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
		// TODO: remove after testing
		confParams = new ArrayList<String>();
		confParams.add("spark.executor.memory");
		confParams.add("spark.executor.cores");
		////////////////////////////////
		for (int i = 0; i < confParams.size(); i++) {
			ConfParam currentParam = TunefulFactory.getSignificanceAnalyzer().getAllParams().get(confParams.get(i));
			writeParamToFile(currentParam, filePath);

		}
	}

	private void writeParamToFile(ConfParam currentParam, String filePath) {
		FileWriter fileWriter;
		try {
			String appModelInputPath = filePath;
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			bufferedWriter.write(currentParam.toJsonString());
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public SparkConf findCandidateConf(SparkConf conf, String appName) {
		// generate pending conf using speamint
		runSpearmint(appName);
		Hashtable<String, String> tunedConf = readPendingConf(appName);
		SparkConf updatedConf = conf;
		Set<String> keys = tunedConf.keySet();
		for (String key : keys) {
			updatedConf.set(key, tunedConf.get(key));
		}

		return updatedConf;
	}

	public void writeToModelInput(SparkConf sparkConf, double cost, String appName) {
		Hashtable conf = getTunableParams(sparkConf, appName);
		FileWriter fileWriter;
		try {
			String appModelInputPath = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			String line = cost + " 0 " + getConfAsstr(conf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String getConfAsstr(Hashtable conf, List<String> confParams) {
		String confAsStr = "";
		for (int i = 0; i < confParams.size(); i++) {
			confAsStr += conf.get(confParams.get(i)) + " ";
		}
		return confAsStr;
	}

	private void writePendingConf(Hashtable conf, String appName) {
		FileWriter fileWriter;
		try {
			String appModelInputPath = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			fileWriter = new FileWriter(appModelInputPath);
			BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			String line = "P P " + getConfAsstr(conf, confParams) + " \\n";
			bufferedWriter.write(line);
			bufferedWriter.flush();
			bufferedWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Hashtable<String, String> readPendingConf(String appName) {
		Hashtable<String, String> conf = new Hashtable<String, String>();
		try {
			String appModelDir = MODEL_INPUT_PATH_BASE + appName + "\\result.dat";
			FileReader fileReader = new FileReader(appModelDir);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line;
			line = bufferedReader.readLine();
			String[] splittedLine = line.split(" ");
			List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
			while (line != null) {
				if (splittedLine.length == confParams.size() + 2) // number of elements per line
				{
					if (line.contains("P")) {
						// parse line into conf and add to the hashtable
						conf = parseLine(line, confParams, appName);

					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return conf;
	}

	private Hashtable<String, String> parseLine(String line, List<String> confParams, String appName) {

		Hashtable<String, String> conf = new Hashtable<String, String>();
		String[] splittedLine = line.split(" ");
		for (int i = 2; i < confParams.size(); i++) {
			conf.put(confParams.get(i - 2), splittedLine[i]);
		}
		return conf;
	}

// get the tunable conf from SparkConf
	private Hashtable<String, String> getTunableParams(SparkConf sparkconf, String appName) {
		Hashtable<String, String> tunableParams = new Hashtable<String, String>();
		List<String> confParams = TunefulFactory.getSignificanceAnalyzer().getSignificantParams(appName);
		for (int i = 0; i < confParams.size(); i++) {
			tunableParams.put(confParams.get(i), sparkconf.get(confParams.get(i)));
		}
		return tunableParams;
	}

	public void updateModel(String appName, SparkConf conf, double cost) {

		writeToModelInput(conf, cost, appName);
		runSpearmint(appName);

	}

	public String getModelPath() {
		return modelPath;
	}

	public void setModelPath(String modelPath) {
		this.modelPath = modelPath;
	}

	private void runSpearmint(String appName) {

		try {

			if (!confFileExists(appName))
				writeParamsJsonFile(appName);
			String appModelDir = MODEL_INPUT_PATH_BASE + appName;
			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHONHOME");
			File file;

			file = new File(CostModeler.class.getResource("/spearmint-lite").toURI().getPath() + "/spearmint-lite.py");
//			File file = new File(Thread.currentThread().getContextClassLoader().getResource("spearmint-lite")
//					.getPath()+"/spearmint-lite.py");

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + pythonFile);
			appModelDir = CostModeler.class.getResource("/spearmint-lite/braninpy").toURI().getPath();
//			appModelDir = Thread.currentThread().getContextClassLoader().getResource("spearmint-lite/braninpy").getPath();
			String cmd = "python " + pythonFile + " --method=GPEIOptChooser --method-args=noiseless=1 " + appModelDir;
			System.out.println("cmd >> " + cmd);

			Process p;
			String[] env = { "PYTHONHOME=" + pythonHome, "PYTHONPATH=" + pythonHome,
					"PATH=/usr/local/opt/python/libexec/bin:$PATH" };
			p = Runtime.getRuntime().exec(cmd, env);
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

	private boolean confFileExists(String appName) {
		File filePath = new File(MODEL_INPUT_PATH_BASE + appName + "\\config.json");
		return (filePath.exists() && !filePath.isDirectory());

	}

	public static void main(String[] args) {
//		new CostModeler().runSpearmint("test");

		try {
			final Map<String, String> envMap = new HashMap<String, String>(System.getenv());
			String pythonHome = envMap.get("PYTHONHOME");
		

			String path = CostModeler.class.getResource("/test.py").getPath();
			System.out.println(">>> path >>>" + path);
			String absPath = new File (path).getAbsolutePath();
			absPath = Thread.currentThread().getContextClassLoader().getResource("test.py").getPath();
			System.out.println(">>> abs path >>>" + absPath);
			File file = new File(path);
//			File file = new File (new CostModeler().getClass().getClassLoader()
//					.getResource("test.py").toURI()
//					.getPath());

			String pythonFile = file.getAbsolutePath();
			System.out.println("file path >> " + path);
//			Runtime.getRuntime().exec(new String[] {pythonHome+"\\python " , pythonFile } );

			Process p;
			String command = "python test.py" ;
//+ path;
                        String pathEnv =envMap.get("PYTHONHOME");

			System.out.println(">>>cmd >>> " + command);
			String[] env = { "PYTHONHOME=" + pythonHome, "PYTHONPATH=" + pythonHome , "PATH="+pathEnv};
			p = Runtime.getRuntime().exec(command, env);
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
		}
//		catch (URISyntaxException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
