package org.willian.owb2infa.model;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

public class MetadataProperty {

	@XStreamAsAttribute
	String name;

	String value;

	@XStreamAsAttribute
	String location;

	@XStreamAsAttribute
	String dataType;

	@XStreamAsAttribute
	private String uoid;

	public MetadataProperty(String name, String value) {
		this.name = name;
		this.value = value;
	}

	public String getData_type() {
		return dataType;
	}

	public void setData_type(String data_type) {
		this.dataType = data_type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getUoid() {
		return uoid;
	}

	public void setUoid(String uoid) {
		this.uoid = uoid;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

}
