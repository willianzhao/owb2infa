package org.willian.owb2infa.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

public class MetadataGroup {

	@XStreamAsAttribute
	private String name;
	@XStreamAsAttribute
	private String direction;
	@XStreamAsAttribute
	private String uoid;

	@XStreamImplicit(itemFieldName = "property")
	private ArrayList<MetadataProperty> properties = new ArrayList<MetadataProperty>();

	@XStreamImplicit(itemFieldName = "field")
	private ArrayList<MetadataField> fields = new ArrayList<MetadataField>();

	public MetadataGroup(String groupName, String groupDirection) {
		name = groupName;
		direction = groupDirection;
	}

	public MetadataField getFieldByName(String fdName) {
		for (MetadataField field : fields) {
			if (field.getName().equalsIgnoreCase(fdName)) {
				return field;
			}
		}

		return null;
	}

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

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	public ArrayList<MetadataField> getFields() {
		return fields;
	}

	public void addToFields(MetadataField column) {
		this.fields.add(column);
	}

	public void setFields(ArrayList<MetadataField> columns) {
		this.fields = columns;
	}

	public ArrayList<MetadataField> getBusinessKeyList() {
		ArrayList<MetadataField> bizKeys = new ArrayList<MetadataField>();

		for (MetadataField field : fields) {
			if (field.getInputConnections() != null
					&& field.getInputConnections().size() > 0
					&& field.isBizKey()) {
				bizKeys.add(field);
			}
		}
		return bizKeys;

	}

	public ArrayList<MetadataField> getToUpdateFields() {
		ArrayList<MetadataField> updateFields = new ArrayList<MetadataField>();

		for (MetadataField field : fields) {
			if (field.getInputConnections() != null && field.isUsedWhenUpdate()
					&& field.getInputConnections().size() > 0) {
				updateFields.add(field);
			}
		}
		return updateFields;

	}

	public MetadataField getPrimaryKey() {

		// Default the primary key only contains one column

		for (MetadataField field : fields) {
			if (field.isKey()) {
				return field;
			}
		}
		return null;
	}

	public ArrayList<MetadataProperty> getProperties() {
		return properties;
	}

	public void addToProperties(MetadataProperty properties) {
		this.properties.add(properties);
	}

	public HashSet<String> getUpwardConnectionNames() {
		// Get the list of incoming connected operators
		HashSet<String> connSet = new HashSet<String>();

		for (MetadataField field : fields) {
			ArrayList<MetadataConnection> inConnections = field
					.getInputConnections();

			for (MetadataConnection conn : inConnections) {
				String opName = conn.getSourceOperator();
				String groupName = conn.getSourceGroup();
				connSet.add(opName + "." + groupName);
			}
		}

		return connSet;
	}
}
