package org.willian.owb2infa.model;

import java.util.ArrayList;
import java.util.HashSet;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

public class MetadataOperator {

	@XStreamAsAttribute
	String type;
	@XStreamAsAttribute
	String name;
	@XStreamAsAttribute
	String businessName;
	@XStreamAsAttribute
	String boundName;

	@XStreamAsAttribute
	private String uoid;

	@XStreamAsAttribute
	boolean source;
	@XStreamAsAttribute
	boolean target;

	String location;

	@XStreamImplicit(itemFieldName = "property")
	private ArrayList<MetadataProperty> properties = new ArrayList<MetadataProperty>();
	@XStreamImplicit(itemFieldName = "group")
	private ArrayList<MetadataGroup> groups = new ArrayList<MetadataGroup>();

	@XStreamOmitField
	private HashSet<String> incomingRelatedOperators = new HashSet<String>();
	@XStreamOmitField
	private HashSet<String> outgoingRelatedOperators = new HashSet<String>();

	public MetadataOperator(String opType, String opName, String busName) {
		type = opType;
		name = opName;
		businessName = busName;
	}

	public MetadataGroup getGroupByName(String gpName) {
		for (MetadataGroup group : groups) {
			if (group.getName().equalsIgnoreCase(gpName)) {
				return group;
			}
		}

		return null;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
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

	public String getBusiness_name() {
		return businessName;
	}

	public void setBusiness_name(String business_name) {
		this.businessName = business_name;
	}

	public ArrayList<MetadataProperty> getProperties() {
		return properties;
	}

	public void addToProperties(MetadataProperty property) {
		this.properties.add(property);
	}

	public ArrayList<MetadataGroup> getGroups() {
		return groups;
	}

	public void addToGroups(MetadataGroup group) {
		this.groups.add(group);
	}

	public boolean isSource() {
		return source;
	}

	public void setSource(boolean source) {
		this.source = source;
	}

	public boolean isTarget() {
		return target;
	}

	public void setTarget(boolean target) {
		this.target = target;
	}

	public HashSet<String> getIncomingRelationships() {
		if (incomingRelatedOperators != null && incomingRelatedOperators.size() > 0) {
			return incomingRelatedOperators;
		} else {

			if (incomingRelatedOperators == null) {
				incomingRelatedOperators = new HashSet<String>();
			}

			for (MetadataGroup group : groups) {
				ArrayList<MetadataField> fields = group.getFields();
				if (fields != null) {
					for (MetadataField field : fields) {
						ArrayList<MetadataConnection> inputConnections = field.getInputConnections();

						if (inputConnections != null) {
							for (MetadataConnection connection : inputConnections) {
								String opName = connection.getSourceOperator();
								String opGroup = connection.getSourceGroup();
								String identifier = opName + "." + opGroup;
								if (!incomingRelatedOperators.contains(identifier)) {
									incomingRelatedOperators.add(identifier);
								}
							}
						}

					}
				}
			}
		}
		return incomingRelatedOperators;
	}

	public HashSet<String> getOutgoingRelationships() {

		if (outgoingRelatedOperators != null && outgoingRelatedOperators.size() > 0) {
			return outgoingRelatedOperators;
		} else {
			if (outgoingRelatedOperators == null) {
				outgoingRelatedOperators = new HashSet<String>();
			}
			for (MetadataGroup group : groups) {
				ArrayList<MetadataField> fields = group.getFields();

				if (fields != null) {
					for (MetadataField field : fields) {
						ArrayList<MetadataConnection> ouputConnections = field.getOutputConnections();

						for (MetadataConnection connection : ouputConnections) {
							String opName = connection.getTargetOperator();
							String opGroup = connection.getTargetGroup();
							String identifier = opName + "." + opGroup;
							if (!outgoingRelatedOperators.contains(identifier)) {
								outgoingRelatedOperators.add(identifier);
							}
						}
					}
				}
			}
		}

		return outgoingRelatedOperators;
	}

	public ArrayList<String> getOutputGroupNames() {
		ArrayList<String> outGroups = new ArrayList<String>();

		for (MetadataGroup group : groups) {
			if (group.getDirection().equals("OUTGRP") || group.getDirection().equals("INOUTGRP")) {
				outGroups.add(group.getName());
			}
		}
		return outGroups;
	}

	public String getLoadingType() {
		String loadingType = null;

		for (MetadataProperty property : properties) {
			if (property.getName().matches(".*LOADING_TYPE.*")) {

				loadingType = property.getValue();

			}
		}

		return loadingType;
	}

	public String getLocation() {
		return location;
	}

	public void setLocation(String location) {
		this.location = location;
	}

	public String getBoundName() {
		return boundName;
	}

	public void setBoundName(String boundName) {
		this.boundName = boundName;
	}
}
