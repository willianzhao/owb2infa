package org.willian.owb2infa.helper.owb;

import java.util.ArrayList;

import org.willian.owb2infa.model.MetadataOperator;
import org.willian.owb2infa.model.MetadataProperty;

public final class OWBPropConstants {

	public static final String HOST = "HOST";
	public static final String PORT = "PORT";
	public static final String ORACLE_SID = "SID";
	public static final String USER = "USER";
	public static final String PASSWORD = "PASSWD";
	public static final String OWB_PROJECT = "PROJECT";
	public static final String OWB_REPOSITORY = "OWBREPO";
	public static final String OWB_MODULE = "MODULE";

	public static boolean isSupportedOWBSourceOperator(String opType) {

		switch (opType) {
			case "TABLE":
			case "VIEW":
			case "EXTERNAL_TABLE":
			case "MATERIALIZED_VIEW":
				return true;
			default:
				return false;
		}
	}

	public static String getOWBOPProperty(MetadataOperator op, String propertyName) {
		ArrayList<MetadataProperty> props = op.getProperties();

		if (props != null) {

			for (MetadataProperty prop : props) {
				if (prop.getName().equalsIgnoreCase(propertyName)) {
					return prop.getValue();
				}
			}
		}

		return null;
	}
}
