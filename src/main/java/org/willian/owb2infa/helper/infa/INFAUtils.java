package org.willian.owb2infa.helper.infa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldKeyType;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.NativeDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTarget;
import com.informatica.powercenter.sdk.mapfwk.core.StringConstants;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationDataTypes;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataField;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;

public class INFAUtils {

	static Logger logger = LogManager.getLogger(INFAUtils.class);

	public static void addGroupToFields(MetadataGroup group,
			List<Field> fields, FieldType type) {

		ArrayList<MetadataField> metadataFields = group.getFields();
		if (metadataFields != null) {
			// create the fields and add them to List
			for (MetadataField metadataField : metadataFields) {

				Field field = convertToField(metadataField, type);
				// field.setGroupName(group.getName());

				fields.add(field);
				logger.debug("Add field " + field.getName() + " ("
						+ field.getDataType() + ", " + field.getPrecision()
						+ ", " + field.getScale() + ") into list");
			}
		}
	}

	public static Field convertToField(MetadataField metadataField,
			FieldType type) {
		String fieldName = metadataField.getBound_name();
		String fieldBusName = metadataField.getBusiness_name();
		String fieldDesc = "Field " + metadataField.getName()
				+ " is migrated from OWB";
		String fieldDataType = "VARCHAR2";

		String fieldType = metadataField.getData_type();

		String precision = null;
		precision = metadataField.getPrecision();

		String databasePrecision = metadataField.getDbColumnPrecision();

		String scale = "0";
		scale = metadataField.getScale();

		if (type == FieldType.TRANSFORM) {

			if (fieldType.equals("VARCHAR") || fieldType.equals("VARCHAR2")
					|| fieldType.equals("CHAR")) {
				fieldDataType = TransformationDataTypes.STRING;
			} else if (fieldType.equals("NUMBER")
					|| fieldType.equals("NUMERIC")
					|| fieldType.equals("DOUBLE") || fieldType.equals("FLOAT")) {

				if (databasePrecision != null) {
					if (databasePrecision.equals("0")) {
						fieldDataType = TransformationDataTypes.DOUBLE;
					} else {
						fieldDataType = TransformationDataTypes.DECIMAL;
					}
				} else {
					fieldDataType = TransformationDataTypes.DOUBLE;
				}
			} else if (fieldType.equals("INTEGER")) {
				fieldDataType = TransformationDataTypes.INTEGER;

			} else if (fieldType.toUpperCase().matches(".*DATE.*")
					|| fieldType.toUpperCase().matches(".*TIME.*")) {
				fieldDataType = TransformationDataTypes.DATE_TIME;
				precision = "29";
				scale = "9";

			} else {

				fieldDataType = TransformationDataTypes.STRING;
			}
		} else {
			fieldType = metadataField.getDbColumnType();

			if (fieldType.equals("VARCHAR") || fieldType.equals("VARCHAR2")
					|| fieldType.equals("CHAR")) {
				fieldDataType = NativeDataTypes.Oracle.VARCHAR2;
			} else if (fieldType.equals("NUMBER")
					|| fieldType.equals("NUMERIC")
					|| fieldType.equals("DOUBLE") || fieldType.equals("FLOAT")) {

				if (databasePrecision != null) {
					if (databasePrecision.equals("0")) {
						fieldDataType = NativeDataTypes.Oracle.NUMBER;
					} else {
						fieldDataType = NativeDataTypes.Oracle.NUMBER_PS;
					}
				} else {
					fieldDataType = NativeDataTypes.Oracle.NUMBER;
				}
			} else if (fieldType.equals("INTEGER")) {
				fieldDataType = NativeDataTypes.Oracle.NUMBER;

			} else if (fieldType.toUpperCase().matches(".*DATE.*")) {
				fieldDataType = NativeDataTypes.Oracle.DATE;
				precision = "19";
				scale = "0";
			} else if (fieldType.toUpperCase().matches(".*TIME.*")) {
				fieldDataType = NativeDataTypes.Oracle.TIMESTAMP;
				precision = "26";
				scale = "6";
			} else {
				fieldDataType = NativeDataTypes.Oracle.VARCHAR2;
			}

			// THIS IS THE BUG! IT WILL THROW FOLLOWING EXCEPTION:
			/*
			 * java.lang.NullPointerException at
			 * com.informatica.powercenter.sdk.
			 * mapfwk.xml.TxPrecisionAndScaleHelper.setPrecisionAndScale(Unknown
			 * Source) at
			 * com.informatica.powercenter.sdk.mapfwk.xml.XMLWriteHelper
			 * .getJaxbTransformField(Unknown Source)
			 */
			// fieldDataType = (fieldType == null ?
			// NativeDataTypes.Oracle.VARCHAR2
			// : fieldType);
		}

		logger.trace("Ready to convert Field : " + fieldName
				+ " , data type : " + fieldType + " -> " + fieldDataType
				+ " , precision : " + precision + " , scale : " + scale);

		Field field = new Field(fieldName, fieldBusName, fieldDesc,
				fieldDataType, precision, scale,
				metadataField.isKey() ? FieldKeyType.PRIMARY_KEY
						: FieldKeyType.NOT_A_KEY, type,
				metadataField.isNullable());

		return field;
	}

	public static ConnectionInfo getRelationalConnectionInfo(
			SourceTargetType dbType, String dbName) {
		ConnectionInfo connInfo = new ConnectionInfo(dbType);
		connInfo.getConnProps().setProperty(ConnectionPropsConstants.DBNAME,
				dbName);
		return connInfo;
	}

	public static String extractFirstPart(String identifier) {
		String identifierFirstPart;

		int pointLoc = identifier.indexOf(".");

		if (pointLoc > 0) {
			identifierFirstPart = identifier.substring(0, pointLoc);
		} else {
			identifierFirstPart = identifier;
		}

		return identifierFirstPart;
	}

	public static String extractSecondPart(String identifier) {
		String identifierSecondPart;

		int pointLoc = identifier.indexOf(".");

		if (pointLoc > 0) {
			identifierSecondPart = identifier.substring(pointLoc + 1);
		} else {
			identifierSecondPart = identifier;
		}

		return identifierSecondPart;
	}

	public static boolean isLoadControlTable(String sourceName) {

		return sourceName.matches(".*LOAD_CONTROL.*");
	}

	public static Source getSourceFromFolder(Folder folder, String sourceName,
			String type) {
		Source source = null;
		ShortCut sc = INFAUtils.getShortCutFromFolder(folder, sourceName, type);
		if (sc != null) {
			source = (Source) sc.getRefObject();
		} else {
			source = folder.getSource(sourceName);
		}
		return source;

	}

	public static SourceTarget getAnySourceTarget(Repository rep,
			final String tableName, final String infaLoc) {
		INFASource infaSource = new INFASource();

		Source source;
		try {
			source = infaSource.getSourceFromShareFolder(rep, tableName,
					infaLoc);
		} catch (RepoOperationException | MapFwkReaderException e1) {
			source = null;
		}

		if (source != null) {

			logger.trace("Find the Source " + tableName
					+ " from the shared folder");
			return source;
		} else {

			INFATarget infaTarget = new INFATarget();
			Target target;

			try {
				target = infaTarget.getTargetFromShareFolder(rep, tableName);

				if (target != null) {

					logger.trace("Find the target " + tableName
							+ " from the shared folder");

					return target;
				}
			} catch (RepoOperationException | MapFwkReaderException e) {
				// bypass

			}

		}

		logger.debug("Failed to get the object " + tableName
				+ " as Source or Target from shared folder");
		return null;
	}

	public static ShortCut getShortCutFromFolder(Folder toCheckFolder,
			final String checkName, String type) {

		logger.debug("Try to get shortcut " + checkName + " from folder "
				+ toCheckFolder.getName());

		List<ShortCut> scList = toCheckFolder.getShortCuts(new INameFilter() {

			public boolean accept(String name) {
				// logger.debug("...Comparing shortcut " + name + "(" +
				// name.length() +
				// ") with " + checkName
				// + " . The result is " + name.matches(".*" + checkName +
				// ".*"));
				// return name.matches(".*" + checkName + ".*");
				return name.equalsIgnoreCase(checkName);
			}
		});

		// List<ShortCut> scList = toCheckFolder.getShortCuts();

		if (scList != null && scList.size() > 0) {

			for (ShortCut sc : scList) {
				// Object object = sc.getRefObject();
				String scName = sc.getName();
				String dbName = sc.getDBName();
				String folderName = sc.getFolderName();
				String objectType = sc.getObjectType();
				logger.trace("The shortcut " + scName + " is in " + folderName
						+ "." + dbName + " as type " + objectType);

				if (type.equals(RepositoryObjectConstants.OBJTYPE_SOURCE)) {
					logger.trace("...Checking source shortcut " + scName);
					if (objectType.equalsIgnoreCase(type)) {

						if (scName.equalsIgnoreCase(checkName)) {
							logger.trace("Found SOURCE shortcut for "
									+ checkName);
							return sc;
						}
					}
				} else if (type
						.equals(RepositoryObjectConstants.OBJTYPE_TARGET)) {

					logger.trace("...Checking target shortcut " + scName);

					if (objectType.equalsIgnoreCase(type)) {

						if (scName.equalsIgnoreCase(checkName)) {
							logger.trace("Found TARGET shortcut for "
									+ checkName);
							return sc;
						}
					}
				}
			}

			// ShortCut shortCut = scList.get(0);
			//
			// logger.trace("The type ("
			// + shortCut.getObjectType()
			// +
			// ") is neither source nor target. So return the first found shortcut");
			//
			// return shortCut;

		} else {
			logger.debug("Can't find the shortcut " + checkName);
		}
		return null;

	}

	public static boolean isSupportedOWBTransform(String opType) {

		switch (opType) {
		case "SEQUENCE":
		case "CONSTANT":
			return false;
		default:
			return true;
		}
		// switch (opType) {
		// case "SOURCE":
		// case "JOINER":
		// case "FILTER":
		// case "LOOKUP":
		// case "EXPRESSION":
		// case "UNION":
		// // case "SEQUENCE":
		// return true;
		// default:
		// return false;
		// }
	}

	public static boolean isOmmitOWBOperatorForFilter(String opType) {

		switch (opType) {
		case "CONSTANT":
		case "EXPRESSION":
			return true;
		default:
			return false;
		}
	}

	public static boolean isSourceExpRowSetExisted(
			HashMap<String, RowSet> expInputSetHM, String opName) {
		boolean found = false;
		if (expInputSetHM.get(opName) != null) {
			found = true;
		}

		return found;
	}

	/*
	 * @param identifier The identifier default should follow the format of
	 * operatorName.outputGroupName
	 */
	public static boolean isTransExpRowSetExisted(
			HashMap<String, RowSet> expInputSetHM, String identifier) {

		boolean found = false;
		if (expInputSetHM.get(identifier) != null) {
			found = true;
		}

		return found;
	}

	public static RowSet getExpRowSet(HashMap<String, RowSet> expInputSetHM,
			String opName) {
		return expInputSetHM.get(opName);
	}

	public static void addToExpRowSetHM(HashMap<String, RowSet> expInputSetHM,
			String opName, RowSet expRowSet) {

		expInputSetHM.put(opName, expRowSet);

		logger.debug("Add export rowsets for transformation " + opName);
	}

	public static RowSet getExpRowSet(MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM, String inIdentifier) {
		String inOPName = INFAUtils.extractFirstPart(inIdentifier);
		String inGroup = INFAUtils.extractSecondPart(inIdentifier);
		MetadataOperator inOP = owbMapping.getOperatorByName(inOPName);

		String lookupName = inIdentifier;

		if (inOP != null) {
			if (inOP.isSource()) {
				logger.trace("The incoming operator " + inOPName
						+ " is a source");
				lookupName = inOPName;

			} else {
				logger.trace("The incoming operator " + inOPName
						+ " is an operator");

				lookupName = inOPName + "." + inGroup;

			}
		}
		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, lookupName);
		return expRS;
	}

	public static RowSet getExpRowSetSimple(
			HashMap<String, RowSet> expInputSetHM, String inIdentifier) {

		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, inIdentifier);
		return expRS;
	}

	public static boolean isFieldExisted(String[] targetArray, String searchItem) {

		for (int i = 0; i < targetArray.length; i++) {
			if (targetArray[i] != null && targetArray[i].equals(searchItem)) {
				return true;
			}
		}

		return false;
	}

	public static boolean isTextType(String dataType) {

		if (dataType.equalsIgnoreCase("nstring")
				|| dataType.equalsIgnoreCase("ntext")
				|| dataType.equalsIgnoreCase("string")
				|| dataType.equalsIgnoreCase("text")
				|| dataType.equalsIgnoreCase("binary")
				|| dataType.matches(".*char.*")
		// || dataType.equalsIgnoreCase("varchar2")
		// || dataType.equalsIgnoreCase("nvarchar2")
		// || dataType.equalsIgnoreCase("varchar")
		// || dataType.equalsIgnoreCase("char")

		) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isNumberType(String dataType) {
		if (dataType.equalsIgnoreCase("double")
				|| dataType.equalsIgnoreCase("decimal")
				|| dataType.equalsIgnoreCase("bigint")
				|| dataType.equalsIgnoreCase("real")
				|| dataType.equalsIgnoreCase("small integer")
				|| dataType.equalsIgnoreCase("integer")
				|| dataType.equalsIgnoreCase("int")
				|| dataType.equalsIgnoreCase("float")
				|| dataType.equalsIgnoreCase("number")
				|| dataType.matches(".*number.*")) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isDateType(String dataType) {
		if (
		// dataType.equalsIgnoreCase("date/time")
		// || dataType.equalsIgnoreCase("datetime")
		// || dataType.equalsIgnoreCase("date")
		dataType.matches(".*date.*") || dataType.equalsIgnoreCase("timestamp")) {
			return true;
		} else {
			return false;
		}
	}

	public static String getTypeDefaultValue(String dataType) {
		String defaultValue = null;

		logger.trace("Accept the input parameter 'dataType' as : " + dataType);
		if (isTextType(dataType)) {
			defaultValue = "' '";

			logger.debug("Get the default as empty string");
		} else if (isNumberType(dataType)) {
			defaultValue = "-999";

			logger.debug("Get the default as number -999");
		} else if (isDateType(dataType)) {
			defaultValue = "sysdate";

			logger.debug("Get the default as sysdate");
		}

		logger.trace("Get the default value (" + defaultValue
				+ ") for data type " + dataType);
		return defaultValue;
	}

	public static boolean isTransformReplacedWithDSQ(
			MetadataMapping owbMapping, MetadataOperator transOP) {

		boolean needReplace = false;

		if (transOP.getType().equals("JOINER")) {
			needReplace = true;

			Iterator<String> inIdentifierIter;
			HashSet<String> incomingRelatedOPs = transOP
					.getIncomingRelationships();
			inIdentifierIter = incomingRelatedOPs.iterator();
			while (inIdentifierIter.hasNext()) {
				String inIdentifier = inIdentifierIter.next();
				String inOPName = INFAUtils.extractFirstPart(inIdentifier);
				MetadataOperator inOP = owbMapping.getOperatorByName(inOPName);

				if (!inOP.isSource()) {

					HashSet<String> incomingRelationships = inOP
							.getIncomingRelationships();
					if (incomingRelationships != null
							&& incomingRelationships.size() > 0) {
						needReplace = false;

						logger.debug("The join operator is connecting to transform "
								+ inOP.getName());
					}
				}
			}
		}
		return needReplace;
	}

	public static String toCamelCase(final String init) {
		if (init == null)
			return null;

		final StringBuilder ret = new StringBuilder(init.length());

		for (final String word : init.split("_")) {
			if (!word.isEmpty()) {
				if (word.equalsIgnoreCase("DW")) {
					ret.append("DW");
				} else {
					ret.append(word.substring(0, 1).toUpperCase());
					ret.append(word.substring(1).toLowerCase());
				}
			}
			if (!(ret.length() == init.length()))
				ret.append("_");
		}

		return ret.toString();
	}

	public static Field tryOtherPossibleName(RowSet rs, String fieldName) {
		HelperBase.printSplitLine(logger);
		logger.trace("Ready to try other alternations for field " + fieldName);
		HelperBase.printSplitLine(logger);

		Field rsField = null;
		String changedName = fieldName;

		logger.trace("Try to parse like <Field>_1 pattern");

		int lastPos = changedName.lastIndexOf("_");

		if (lastPos != -1) {
			changedName.substring(0, lastPos);

			logger.trace("Try field name " + changedName);
			rsField = rs.getField(changedName);

		}

		if (rsField == null) {
			logger.trace("Try to parse like <Field>[digit] pattern for at most two digits.");
			changedName = fieldName;

			int i = 1;
			int length = fieldName.length();
			char lastChar = fieldName.charAt(length - i);

			if (Character.isDigit(lastChar)) {
				logger.trace("Try to remove the last char");
				changedName = fieldName.substring(0, length - i);
				rsField = rs.getField(changedName);

				if (rsField == null) {
					logger.trace("Try to remove the last two chars");

					i++;
					changedName = fieldName.substring(0, length - i);
					rsField = rs.getField(changedName);

					if (rsField == null) {
						logger.trace("Faild to get the field by name as "
								+ changedName);

					} else {
						logger.trace("Get field by name as " + changedName);
					}
				} else {
					logger.trace("Get field by name as " + changedName);
				}
			}
		} else {
			logger.trace("Get field by name as " + changedName);
		}

		return rsField;

	}

	/*
	 * This method is to get all the keys appearing in the joining condition and
	 * construct sort key list.
	 */
	public static ArrayList<String> getSortKey(RowSet rs, String condition) {

		ArrayList<String> sortKeys = new ArrayList<String>();

		logger.debug("Ready to parse condition string as : " + condition);
		String originalString = condition.trim().toUpperCase();
		originalString = originalString.replace("(", "");
		originalString = originalString.replace("\\+", "");
		originalString = originalString.replace(")", "");

		logger.trace("Convert to uppercase : " + originalString);

		String[] clauses = originalString.split("AND|OR|BETWEEN");

		if (clauses != null && clauses.length > 0) {
			logger.trace("Get " + clauses.length
					+ " clauses by replacing AND, OR, BETWEEN");
			for (String clause : clauses) {

				logger.trace("Processing clause string : " + clause);

				String[] terms = clause.split("=");
				if (terms != null) {
					logger.trace("Get " + terms.length + " terms");

					if (terms.length == 2) {

						boolean foundKey = false;

						String leftTerm = terms[0].trim();
						logger.trace("Get left term as : " + leftTerm);

						String[] pairs = leftTerm.split("\\.");

						if (pairs != null && pairs.length == 2) {

							String column = pairs[1].trim();

							Field dummyField = rs.getField(column);

							if (dummyField != null) {
								sortKeys.add(column);

								foundKey = true;

								logger.debug("Add the sort key " + column);

							} else {
								logger.trace("Can't find field in the rowset for "
										+ column);
							}
						}

						if (!foundKey) {

							String rightTerm = terms[1].trim();
							logger.trace("Get right term as : " + rightTerm);

							pairs = leftTerm.split("\\.");

							if (pairs != null && pairs.length == 2) {

								String column = pairs[1].trim();

								Field dummyField = rs.getField(column);

								if (dummyField != null) {
									sortKeys.add(column);
									logger.debug("Add the sort key " + column);
								} else {
									logger.trace("Can't find field in the rowset for "
											+ column);
								}
							} else {
								logger.trace("Can't get valid column from this clause. Skip it");
							}
						}
					}
				} else {
					logger.trace("Failed to split the term string '" + clause
							+ "' by = ");
				}
			}
		}

		return sortKeys;
	}
}
