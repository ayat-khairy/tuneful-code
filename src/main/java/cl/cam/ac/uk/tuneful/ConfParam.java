package cl.cam.ac.uk.tuneful;

public class ConfParam {

	private String name;
	private String type;
	private double[] range;
	private String[] values;
	private String unit;

	public ConfParam(String name, String type, double[] range, String[] values, String unit) {

		this.name = name;
		this.type = type;
		this.range = range;
		this.values = values;
		this.unit = unit;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public double[] getRange() {
		return range;
	}

	public void setRange(double[] range) {
		this.range = range;
	}

	public String[] getValues() {
		return values;
	}

	public void setValues(String[] values) {
		this.values = values;
	}

	public String getUnit() {
		return unit;
	}

	public void setUnit(String unit) {
		this.unit = unit;
	}

	public String toJsonString() {
		String str = "\"" + name +"\":{" + "\"name\":\"" + name + "\",\n";
		if (type.equals("bolean") || type.equals("enum")) {
			str += "\"type\":\"enum\"" + ",\n\"options\":" + values;
		} else if (type.equals("int")) {
			str += "\n\"type\":\"" + type + "\",\n \"min\":" + (int) range[0] + ",\n \"max\":" + (int) range[1];
		} else {
			str += "\n \"type\":\"" + type + "\",\n \"min\":" + range[0] + ",\n \"max\":" + range[1];
		}
		str += ",\n\"size\":1\n}";
		return str;
	}

}
