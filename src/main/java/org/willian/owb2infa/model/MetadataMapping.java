
package org.willian.owb2infa.model;

import java.util.ArrayList;
import java.util.List;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;


@XStreamAlias("mapping")
public class MetadataMapping {

	@XStreamAsAttribute
	private String generationLanguage;

	@XStreamAsAttribute
	private String uoid;

	@XStreamAsAttribute
	private String name;

	@XStreamAsAttribute
	private String businessName;

	@XStreamImplicit(itemFieldName = "operator")
	private ArrayList<MetadataOperator> operators = new ArrayList<MetadataOperator>();

	@XStreamOmitField
	private ArrayList<MetadataOperator> onlyOutputOperators = new ArrayList<MetadataOperator>();

	@XStreamOmitField
	private ArrayList<MetadataOperator> onlyTransformOperators = new ArrayList<MetadataOperator>();

	public MetadataMapping(String codeType, String mappingName, String busName) {
		generationLanguage = codeType;
		name = mappingName;
		businessName = busName;
	}

	public MetadataOperator getOperatorByName(String opName) {
		for (MetadataOperator op : operators) {
			String operatorName = op.getName();
			if (operatorName.equalsIgnoreCase(opName)) {
				return op;
			}
			else {
				String operatorBoundName = op.getBoundName();
				if (operatorBoundName != null) {

					//The bound name has the higher priority in search path
					if (operatorBoundName.equalsIgnoreCase(opName)) {
						return op;
					}
				} 
			}
		}

		return null;
	}

	public String getGeneration_language() {
		return generationLanguage;
	}

	public void setGeneration_language(String generation_language) {
		this.generationLanguage = generation_language;
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

	public ArrayList<MetadataOperator> getOperators() {
		return operators;
	}

	public void addToOperators(MetadataOperator operator) {
		this.operators.add(operator);
	}

	public List<MetadataOperator> getOnlyOutputOperators() {

		if (onlyOutputOperators != null && onlyOutputOperators.size() > 0) {
			return onlyOutputOperators;
		} else {

			if (onlyOutputOperators == null) {
				onlyOutputOperators = new ArrayList<MetadataOperator>();
			}
			for (MetadataOperator operator : getTransformOperators()) {

				List<MetadataGroup> groups = operator.getGroups();
				boolean foundInputGroup = false;
				for (MetadataGroup group : groups) {
					if (!foundInputGroup && group.getDirection().contains("IN")) {
						foundInputGroup = true;
					}
				}
				if (!foundInputGroup) {
					onlyOutputOperators.add(operator);
				}

			}
		}
		return onlyOutputOperators;
	}

	public List<MetadataOperator> getTransformOperators() {
		if (onlyTransformOperators != null && onlyTransformOperators.size() > 0) {
			return onlyTransformOperators;
		} else {

			if (onlyTransformOperators == null) {
				onlyTransformOperators = new ArrayList<MetadataOperator>();
			}

			for (MetadataOperator operator : operators) {

				if (!operator.isSource() && !operator.isTarget()) {

					onlyTransformOperators.add(operator);
				}
			}
		}
		return onlyTransformOperators;
	}
}
