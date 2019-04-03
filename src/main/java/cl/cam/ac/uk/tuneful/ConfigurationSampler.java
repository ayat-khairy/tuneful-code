package cl.cam.ac.uk.tuneful;

import java.util.Hashtable;
import java.util.List;

import org.apache.commons.math3.random.SobolSequenceGenerator;

import cl.cam.ac.uk.tuneful.util.TunefulFactory;
import cl.cam.ac.uk.tuneful.util.Util;

public class ConfigurationSampler {
	List<String> allparamsNames = null;
	Hashtable<String, ConfParam> paramRanges = null;
	Hashtable<String, SobolSequenceGenerator> samplers = null;
	Hashtable<String, Integer> samplersIndices = null;
	String samplersPath;

	public ConfigurationSampler() {
		samplersPath = TunefulFactory.getTunefulHome() + "/samplersIndicies.ser";
		allparamsNames = TunefulFactory.getTunableParams();
		paramRanges = TunefulFactory.getTunableParamsRange();
		samplers = new Hashtable<String, SobolSequenceGenerator>();
		samplersIndices = new Hashtable<String, Integer>();
		// new SobolSequenceGenerator(paramsNames.size());
		samplersIndices= Util.loadTable(samplersPath);
		System.out.println(">>> sample indices table size >>> " + samplersIndices.size());
	}

	public Hashtable<String, String> getNextSample(String appName) {
		double[] sample = samplers.get(appName).nextVector();
		Util.writeTable(samplers, samplersPath);
		Hashtable<String, String> sampleConf = mapSampleToConf(sample, allparamsNames);
		return sampleConf;

	}

	private Hashtable<String, String> mapSampleToConf(double[] sample, List<String> params) {
		System.out.println(">> sample >> " + sample);
		Hashtable<String, String> sampledConf = new Hashtable<String, String>();
		for (int i = 0; i < params.size(); i++) {
			ConfParam currentParam = paramRanges.get(params.get(i).trim());
			System.out.println(">> currentParam >>> " + currentParam.getName() );
			if (currentParam.getType().equalsIgnoreCase("int")) {
				// map to range
				int conf = (int) (currentParam.getRange()[0]
						+ (sample[i] * (currentParam.getRange()[1] - currentParam.getRange()[0])));
				// add paramName and the mapped value and unit to the hashtable
				sampledConf.put(currentParam.getName(), conf + currentParam.getUnit());
			} else if (currentParam.getType().equalsIgnoreCase("float")) {
				// map to range
				double conf = (double) (currentParam.getRange()[0]
						+ (sample[i] * (currentParam.getRange()[1] - currentParam.getRange()[0])));
				// add paramName and the mapped value and unit to the hashtable
				sampledConf.put(currentParam.getName(), conf + currentParam.getUnit());
			}

			else if (currentParam.getType().equalsIgnoreCase("boolean")) {
				// map to true or false
				boolean conf = (sample[i] >= 0.5);
				// add paramName and the mapped value and unit to the hashtable
				sampledConf.put(currentParam.getName(), conf + "");
			} else // enum
			{
				// use the values not the ranges
				int conf = (int) (sample[i] * (currentParam.getValues().length));// multply with the cong range
				String confAsSTr = currentParam.getValues()[conf];
				// add paramName and the mapped value and unit to the hashtable
				sampledConf.put(currentParam.getName(), confAsSTr);

			}

		}

		return sampledConf;
	}

	private String getAvgValue(String paramName) {

		// leave enum and boolean param as defaults and do not set
		// if param type is boolean or enum, set to default, else set to the mean
//		if (paramRanges.get(paramName).getType().equalsIgnoreCase("boolean")
//				|| paramRanges.get(paramName).getType().equalsIgnoreCase("boolean"))
//			return getDefaultValue(paramName);
//		else 
		if (paramRanges.get(paramName).getType().equalsIgnoreCase("int")) {
			return ((paramRanges.get(paramName).getRange()[0]
					+ (paramRanges.get(paramName).getRange()[1] - paramRanges.get(paramName).getRange()[0]) / 2))
					+ paramRanges.get(paramName).getUnit(); // start_range + range/2
		} else if (paramRanges.get(paramName).getType().equalsIgnoreCase("float")) {
			return ((paramRanges.get(paramName).getRange()[0]
					+ (paramRanges.get(paramName).getRange()[1] - paramRanges.get(paramName).getRange()[0]) / 2.0))
					+ paramRanges.get(paramName).getUnit();
		}
		return null;
	}

//	private String getDefaultValue(String paramName) {
//		// TODO Auto-generated method stub
//		return null;
//	}

	public Hashtable<String, String> sample(String appName, List<String> sigParams) {
		int index = 0;
		if (samplersIndices.containsKey(appName)) {
			index =samplersIndices.get(appName);
		}
		System.out.println(">>> current sampling index >>> " + index);
		double[] sample = new SobolSequenceGenerator(sigParams.size()).skipTo(index);
		samplersIndices.put(appName, index+1);
		Util.writeTable(samplersIndices, samplersPath);
		Hashtable<String, String> sampleConf = mapSampleToConf(sample, sigParams);
		System.out.println("sample >>> " + sample);
		System.out.println("mapped conf >>> " + sampleConf);
		if (sigParams.size() < allparamsNames.size()) {
			for (int i = 0; i < allparamsNames.size(); i++) {
				String param = allparamsNames.get(i);
				if (!sampleConf.containsKey(param) && getAvgValue(param) != null) { // fixed param and param other than
																					// bool and enum, enum and bool will
																					// have null avg value
					sampleConf.put(param, getAvgValue(param));
				}
			}
		}
		return sampleConf;

	}

	public void createNewSampler(String appName, int size) {
		samplers.put(appName, new SobolSequenceGenerator(size));
//		samplersIndices.put(appName, samplers.get(appName).getNextIndex());

	}

	public void resetIndex(String appName) {
		samplersIndices.put(appName, 0);
		Util.writeTable(samplersIndices, samplersPath);
	}

}
