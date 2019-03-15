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
		String str = "{" + name +"={" + "name='" + name + '\'';
		if (type.equals("bolean") || type.equals("enum")) {
			str += ", type=" + "enum" + ", options=" + values;
		} else if (type.equals("int")) {
			str += ", type=" + type + ", min=" + (int) range[0] + ", max=" + (int) range[1];
		} else {
			str += ", type=" + type + ", min=" + range[0] + ", max=" + range[1];
		}
		str += "}}";
		return str;
	}

}
