package org.willian.owb2infa.model;

import java.util.ArrayList;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

public class MetadataField {
	@XStreamAsAttribute
	String uoid;

	String position;
	String name;
	String businessName;
	String boundName;
	String description;

	String dataType;
	String precision;
	String scale;

	String expression;

	String dbColumnType;
	String dbColumnPrecision;
	String dbColumnScale;

	String defaultValue;

	boolean isKey = false;
	boolean nullable = false;

	boolean isBizKey = false;
	boolean isUsedWhenUpdate = false;

	@XStreamImplicit(itemFieldName = "property")
	private ArrayList<MetadataProperty> properties = new ArrayList<MetadataProperty>();

	@XStreamImplicit(itemFieldName = "outputConnection")
	private ArrayList<MetadataConnection> outpuConnections = new ArrayList<MetadataConnection>();

	@XStreamImplicit(itemFieldName = "inputConnection")
	private ArrayList<MetadataConnection> inputConnections = new ArrayList<MetadataConnection>();

	public String getUoid() {
		return uoid;
	}

	public void setUoid(String uoid) {
		this.uoid = uoid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescription() {
		if (description != null) {
			return description;
		} else {
			return name;
		}
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getPrecision() {

		if (precision == null || precision.equals("0")) {

			if (dbColumnPrecision != null && !dbColumnPrecision.equals("0")) {
				precision = dbColumnPrecision;

			} else {
				if (dataType != null) {
					if (dataType.equals("INTEGER") || dataType.equals("NUMBER") || dataType.equals("NUMERIC")
							|| dataType.equals("FLOAT") || dataType.equals("DOUBLE")) {
						precision = "15";
						// } else if (dataType.equals("DATE")) {
						// precision = "19";
					} else if (dataType.equals("VARCHAR2") || dataType.equals("CHAR")) {
						precision = "255";
					} else if (dataType.equals("DATE") || dataType.equals("TIMESTAMP")) {
						precision = "19";

					} else {
						precision = "1";
					}

				} else {
					if (dbColumnType.equals("INTEGER") || dbColumnType.equals("NUMBER")
							|| dbColumnType.equals("NUMERIC") || dbColumnType.equals("FLOAT")
							|| dbColumnType.equals("DOUBLE") || dbColumnType.equals("VARCHAR2")
							|| dbColumnType.equals("CHAR") || dbColumnType.equals("VARCHAR")
							|| dbColumnType.equals("DATE") || dbColumnType.equals("TIMESTAMP")) {

						precision = dbColumnPrecision;
					} else {
						precision = "1";
					}

				}
			}
		}

		return precision;
	}

	public void setPrecision(String precision) {
		this.precision = precision;
	}

	public String getScale() {

		if (scale == null || scale.equals("0")) {
			// if (dataType.equals("DATE")) {
			// scale = "9";
			// } else {
			scale = "0";
			// }
		}

		return scale;
	}

	public void setScale(String scale) {
		this.scale = scale;
	}

	public boolean isKey() {
		return isKey;
	}

	public void markAsKey(boolean isKey) {
		this.isKey = isKey;
	}

	public boolean isNullable() {
		return nullable;
	}

	public void setNullable(boolean nullable) {
		this.nullable = nullable;
	}

	public String getPosition() {
		if (position != null) {
			return position;
		} else {
			return "9999";
		}
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getBusiness_name() {

		if (businessName != null) {
			return businessName;
		} else {
			return name;
		}
	}

	public void setBusiness_name(String business_name) {
		this.businessName = business_name;
	}

	public String getBound_name() {
		if (boundName != null) {
			return boundName;
		} else {
			return name;
		}
	}

	public void setBound_name(String bound_name) {
		this.boundName = bound_name;
	}

	public String getData_type() {
		if (dataType != null) {
			return dataType;
		} else {
			return dbColumnType;
		}

	}

	public void setData_type(String data_type) {
		this.dataType = data_type;
	}

	public String getExpression() {

		// TODO: format the string and remove some escape chars

		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public ArrayList<MetadataConnection> getOutputConnections() {
		if (outpuConnections == null) {
			outpuConnections = new ArrayList<MetadataConnection>();
		}
		return outpuConnections;
	}

	public void addToOutputConnections(MetadataConnection connection) {
		if (outpuConnections == null) {
			outpuConnections = new ArrayList<MetadataConnection>();
		}
		this.outpuConnections.add(connection);
	}

	public ArrayList<MetadataConnection> getInputConnections() {

		if (inputConnections == null) {
			inputConnections = new ArrayList<MetadataConnection>();
		}
		return inputConnections;
	}

	public ArrayList<MetadataProperty> getProperties() {
		return properties;
	}

	public void addToProperties(MetadataProperty properties) {
		this.properties.add(properties);
	}

	public void addToInputConnections(MetadataConnection connection) {
		if (inputConnections == null) {
			inputConnections = new ArrayList<MetadataConnection>();
		}
		this.inputConnections.add(connection);
	}

	public String getDbColumnType() {
		return dbColumnType;
	}

	public void setDbColumnType(String dbColumnType) {
		this.dbColumnType = dbColumnType;
	}

	public String getDbColumnPrecision() {
		return dbColumnPrecision;
	}

	public void setDbColumnPrecision(String dbColumnPrecision) {
		this.dbColumnPrecision = dbColumnPrecision;
	}

	public String getDbColumnScale() {
		return dbColumnScale;
	}

	public void setDbColumnScale(String dbColumnScale) {
		this.dbColumnScale = dbColumnScale;
	}

	public boolean isBizKey() {
		return isBizKey;
	}

	public void setBizKey(boolean isBizKey) {
		this.isBizKey = isBizKey;
	}

	public boolean isUsedWhenUpdate() {
		return isUsedWhenUpdate;
	}

	public void setUsedWhenUpdate(boolean isUsedWhenUpdate) {
		this.isUsedWhenUpdate = isUsedWhenUpdate;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}
}
