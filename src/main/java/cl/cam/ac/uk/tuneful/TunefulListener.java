package cl.cam.ac.uk.tuneful;

import java.util.Hashtable;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;

public class TunefulListener extends SparkListener {

	private SparkConf sparkConf;
	private long appStartTime;
	private String appName;

	public TunefulListener(SparkConf conf) {
		this.sparkConf = conf;
	}

	@Override
	public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
		System.out.println(">>>>>>> tuneful Listener >>>> on application start ...");
		appName = applicationStart.appName().replaceAll(" ", "");  // get rid of any spaces in the appName
		
		appStartTime = applicationStart.time();

		// set the values in the sparkConf
		if (!TunefulFactory.getSignificanceAnalyzer().isSigParamDetected(appName)) {
			System.out.println(">>> Sig param are not detected yet ...");
			// get the conf using SA
			Hashtable<String, String> conf = TunefulFactory.getSignificanceAnalyzer().suggestNextConf(appName);
			
			Set<String> keys = conf.keySet();
			for (String key : keys) {
				sparkConf.set(key, conf.get(key));
				System.out.println(">>> " + key + " >>> " + conf.get(key)) ;
			}
		}

		else {
			// get the conf using cost modeler
			System.out.println(">>> Sig param detected ... get conf using cost modeler");
			sparkConf = TunefulFactory.getCostModeler().findCandidateConf(sparkConf, appName);

		}

	}

	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
		System.out.println(">>>>>>> tuneful Listener >>>> on application End ...");
		long execTime =  applicationEnd.time() - appStartTime;
		if (!TunefulFactory.getSignificanceAnalyzer().isSigParamDetected(appName)) {
			System.out.println(">>> Application end ... Sig param are not detected yet ..");
			// write conf anf execTime to SA input file
			TunefulFactory.getSignificanceAnalyzer().storeAppExecution(sparkConf, execTime, appName);
		}

		else {
			System.out.println(">>> Application End ... Sig param detected, updating cost modeler");
			// write conf anf execTime to GP file
			TunefulFactory.getCostModeler().writeToModelInput(sparkConf, execTime, appName);

		}

	}
}
