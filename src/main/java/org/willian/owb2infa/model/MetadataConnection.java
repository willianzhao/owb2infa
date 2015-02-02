package org.willian.owb2infa.model;

import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

public class MetadataConnection {

	@XStreamAsAttribute
	String sourceOUID;
	
	@XStreamAsAttribute
	String targetOUID;

	String sourceField;
	String sourceGroup;
	String sourceOperator;

	String targetField;
	String targetGroup;
	String targetOperator;

	public MetadataConnection(String srcOuid, String tgtOuid) {
		sourceOUID = srcOuid;
		targetOUID = tgtOuid;
	}

	public String getSourceField() {
		return sourceField;
	}

	public void setSourceField(String sourceField) {
		this.sourceField = sourceField;
	}

	public String getSourceGroup() {
		return sourceGroup;
	}

	public void setSourceGroup(String sourceGroup) {
		this.sourceGroup = sourceGroup;
	}

	public String getSourceOperator() {
		return sourceOperator;
	}

	public void setSourceOperator(String sourceOperator) {
		this.sourceOperator = sourceOperator;
	}

	public String getSourceOUID() {
		return sourceOUID;
	}

	public void setSourceOUID(String sourceOUID) {
		this.sourceOUID = sourceOUID;
	}

	public String getTargetOUID() {
		return targetOUID;
	}

	public void setTargetOUID(String targetOUID) {
		this.targetOUID = targetOUID;
	}

	public String getTargetField() {
		return targetField;
	}

	public void setTargetField(String targetField) {
		this.targetField = targetField;
	}

	public String getTargetGroup() {
		return targetGroup;
	}

	public void setTargetGroup(String targetGroup) {
		this.targetGroup = targetGroup;
	}

	public String getTargetOperator() {
		return targetOperator;
	}

	public void setTargetOperator(String targetOperator) {
		this.targetOperator = targetOperator;
	}

}
