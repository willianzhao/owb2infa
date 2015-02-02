package org.willian.owb2infa.helper.infa;

public class INFAPropConstants {

	public static String getINFATransPrefix(String opType) {
		String infaTransPrefix = "";

		switch (opType) {
		case "JOINER":
			infaTransPrefix = "JNR_";
			break;
		case "FILTER":
			infaTransPrefix = "FLT_";
			break;
		case "LOOKUP":
			infaTransPrefix = "LKP_";
			break;
		case "EXPRESSION":
			infaTransPrefix = "EXP_";
			break;
		case "UNION":
			infaTransPrefix = "UN_";
			break;
		case "SOURCE":
			infaTransPrefix = "SQ_";
			break;
		case "UPSERT":
			infaTransPrefix = "UPD_";
			break;
		case "SHORTCUT":
			infaTransPrefix = "SC_";
			break;
		case "SQL":
			infaTransPrefix = "SQL_";
			break;
		case "UNPIVOT":
		case "PIVOT":
			infaTransPrefix = "NRM_";
			break;
		case "AGGR":
			infaTransPrefix = "AGG_";
			break;
		case "SORT":
			infaTransPrefix = "SRT_";
			break;
		case "TARGET":
			 infaTransPrefix = "TGT_";
			 //infaTransPrefix = "";
			break;
		case "FUNCTION":
			infaTransPrefix = "SP_";
			break;
		case "SPLITTER":
			infaTransPrefix = "RTR_";
			break;
		case "UNSUPPORTED":
			infaTransPrefix = "XXX_";
			break;
		default:
		}

		return infaTransPrefix;
	}

	public static String getINFALocation(String type, String owbLocation) {
		String infaLoc = "LOCALDB";

		if (owbLocation == null || owbLocation.equals("*")) {
			infaLoc = "*";
		} else if (owbLocation != null) {

			if (type.equals("SOURCE")) {
				switch (owbLocation) {

				case "PRODUCTION_SOURCE":				
				case "STUB_FEED_SRC":
					infaLoc = "SourceEcommReplica";
					break;
				case "WAREHOUSE_TARGET":
					infaLoc="TargetDW";
					break;
				default:
					infaLoc = "LOCALDB";
				}
			} else {
				switch (owbLocation) {

				case "PRODUCTION_SOURCE":
				case "WAREHOUSE_TARGET":
					infaLoc = "TargetDW";
					break;
				case "STUB_FEED_SRC":
					infaLoc = "SourceEcommReplica";
					break;
				default:
					infaLoc = "LOCALDB";
				}
			}
		}
		return infaLoc;
	}
}
