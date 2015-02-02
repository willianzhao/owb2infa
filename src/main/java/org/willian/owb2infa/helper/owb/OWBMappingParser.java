package org.willian.owb2infa.helper.owb;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.helper.db.OracleHelper;
import org.willian.owb2infa.model.MetadataConnection;
import org.willian.owb2infa.model.MetadataField;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;
import org.willian.owb2infa.model.MetadataProperty;
import com.thoughtworks.xstream.XStream;
import oracle.owb.foundation.OWBException;
import oracle.owb.foundation.OWBNamedObject;
import oracle.owb.foundation.property.NoSuchPropertyException;
import oracle.owb.foundation.property.TypeMismatchException;
import oracle.owb.mapping.Map;
import oracle.owb.mapping.MapAttribute;
import oracle.owb.mapping.MapAttributeGroup;
import oracle.owb.mapping.MapAttributeGroupImpl;
import oracle.owb.mapping.MapAttributeImpl;
import oracle.owb.mapping.MapOperator;
import oracle.owb.mapping.MapOperatorImpl;
import oracle.owb.mapping.Mappable;
import oracle.owb.oracle.OracleModule;

public class OWBMappingParser extends Base {

	OracleModule module;
	String xmlOutputPath;
	private Logger logger = LogManager.getLogger(OWBMappingParser.class);

	public OWBMappingParser(String outputPath) throws IllegalArgumentException, IOException {
		super();
		String moduleName = properties.getProperty(OWBPropConstants.OWB_MODULE);
		module = getOracleModuleByName(moduleName);
		xmlOutputPath = outputPath;
	}

	public OWBMappingParser(String moduleName, String outputPath) throws IllegalArgumentException, IOException {
		super();
		module = getOracleModuleByName(moduleName);
		xmlOutputPath = outputPath;

		logger.debug("Construct owb metadata parser with module (" + moduleName + ") and xml output path ("
				+ outputPath + ")");
	}

	public ArrayList<String> parseMappingByPattern(String pattern) {
		StringBuffer additionalInfo = new StringBuffer();

		ArrayList<String> parsedMappingNames = new ArrayList<String>();
		ArrayList<String> matchedMappingNames = getMatchedMappingNames(pattern);

		if (matchedMappingNames.size() > 0) {
			logger.debug("Find " + matchedMappingNames.size() + " mappings for the pattern '" + pattern
					+ "' in OWB repository");

			additionalInfo.append("========Success Parsed Mapping List========\n");
			for (String mappingName : matchedMappingNames) {
				logger.debug("...Mapping " + mappingName + " matches the pattern (" + pattern + "). Start to parse it.");
				try {
					parseMapping(mappingName, additionalInfo);

					parsedMappingNames.add(mappingName);
				} catch (IllegalArgumentException | IOException | SQLException e) {
					logger.error(HelperBase.getStackTrace(e));
					/*
					 * gracefulExit(); System.exit(1);
					 */
					// if any error happened, proceed to the next one

					logger.error("Failed to parse OWB mapping " + mappingName);
					continue;
				}

				try {
					saveAdditionalInfo(additionalInfo);
				} catch (Exception e) {
					logger.error(HelperBase.getStackTrace(e));
					/*
					 * gracefulExit(); System.exit(1);
					 */
					logger.error("Failed to save OWB mapping " + mappingName + " additional information");
				}

			}

		} else {
			logger.debug("There is no matched mapping for pattern '" + pattern + "' in OWB repository");
		}

		return parsedMappingNames;
	}

	public ArrayList<String> getMatchedMappingNames(String pattern) {
		ArrayList<String> matchedMappingNames = new ArrayList<String>();

		String[] mappingNames = module.getMapNames();
		for (String mappingName : mappingNames) {
			if (mappingName.matches(pattern)) {
				matchedMappingNames.add(mappingName);
			}
		}
		return matchedMappingNames;
	}

	public void saveAdditionalInfo(StringBuffer additionalInfo) throws IOException {
		PrintWriter pw = getPrintWriter(xmlOutputPath, "AdditionalInfo.txt");
		pw.println(additionalInfo.toString());
		pw.close();
	}

	public ArrayList<String> parseAllMappings() {

		ArrayList<String> parsedMappingNames = new ArrayList<String>();

		logger.debug("Parsing all the mappings");

		parsedMappingNames = parseMappingByPattern(".*");

		return parsedMappingNames;
	}

	public void parseMapping(String mappingName, StringBuffer additionalInfo) throws IllegalArgumentException,
			IOException, SQLException {

		logger.debug("Parsing mapping " + mappingName);

		Map owbMap = module.findMap(mappingName);

		if (owbMap != null) {
			MetadataMapping mapping = constructMappingMetadata(mappingName, owbMap);

			MapOperator[] mapOperators = owbMap.getMapOperators();

			if (mapOperators != null && mapOperators.length > 0) {
				for (MapOperator op : mapOperators) {
					addOperatorInfo(mapping, op);
				}

				sortByType(mapping.getOperators());
				saveXMLtoLocal(mappingName, mapping);
				/*
				 * String seqInfo = parseSequenceInfo(mapping);
				 * 
				 * if (seqInfo != null) { additionalInfo.append(seqInfo); }
				 */
				// To record the success processed mapping
				additionalInfo.append(mappingName + "\n");
			}
		} else {
			logger.error("Can't find mapping " + mappingName + " in OWB");
			throw new IOException("Mapping " + mappingName + " is missing");
		}
	}

	public String parseSequenceInfo(MetadataMapping mapping) {

		String seperator = ",";
		StringBuffer sb = new StringBuffer();
		boolean foundSeq = false;

		ArrayList<MetadataOperator> owbOps = mapping.getOperators();
		// HashMap<String, RowSet> outputSetHM = new HashMap<String, RowSet>();
		for (MetadataOperator op : owbOps) {
			if (op.getType().equals("SEQUENCE")) {

				sb.append(mapping.getName() + seperator);

				ArrayList<MetadataGroup> groups = op.getGroups();

				for (MetadataGroup group : groups) {
					ArrayList<MetadataField> metadataFields = group.getFields();

					// create the fields and add them to List
					for (MetadataField metadataField : metadataFields) {
						ArrayList<MetadataConnection> oConns = metadataField.getOutputConnections();

						for (MetadataConnection oConn : oConns) {
							foundSeq = true;

							sb.append(op.getName() + seperator);
							sb.append(oConn.getTargetOperator() + seperator);
							sb.append(oConn.getTargetField());
							sb.append("\n");
						}
					}
				}
			}

		}

		if (foundSeq) {

			return sb.toString();
		}

		return null;
	}

	private void saveXMLtoLocal(String mappingName, MetadataMapping mapping) {
		XStream xstream = new XStream();

		xstream.processAnnotations(MetadataMapping.class);

		String xml = xstream.toXML(mapping);

		try {

			String mappingBizName = mapping.getBusiness_name();
			PrintWriter pw = getPrintWriter(xmlOutputPath, mappingBizName + ".xml");
			pw.print(xml);

			pw.close();
		} catch (IOException e) {
			logger.error(HelperBase.getStackTrace(e));
			// gracefulExit();
			// System.exit(1);
		}
	}

	private MetadataMapping constructMappingMetadata(String mappingName, Map owbMap) {

		String mapBusName = owbMap.getBusinessName();
		String codeType = null;
		String uoid = owbMap.getUOID();
		try {
			codeType = owbMap.getPropertyValueString("GENERATION_LANGUAGE");
		} catch (NoSuchPropertyException | TypeMismatchException e) {
			codeType = "PLSQL";
		}

		MetadataMapping mapping = new MetadataMapping(codeType, mapBusName, mappingName);
		mapping.setUoid(uoid);
		return mapping;
	}

	private void addOperatorInfo(MetadataMapping mapping, MapOperator op) throws IllegalArgumentException, IOException,
			SQLException {

		String opBusinessName = null;
		String opName = null;
		String uoid;
		String opType = op.getOperatorType();
		opName = op.getName();
		opBusinessName = op.getBusinessName();
		uoid = op.getUOID();

		boolean processed = false;

		logger.debug("Add " + opName + " operator infor");

		MetadataOperator operator = new MetadataOperator(opType, opName, opBusinessName);
		operator.setUoid(uoid);

		if (OWBPropConstants.isSupportedOWBSourceOperator(opType)) {
			OWBNamedObject boundObject = op.getBoundObject();

			if (boundObject != null) {
				String boundName = boundObject.getName();
				operator.setBoundName(boundName);

				String moduleName = op.getMap().getModule().getName();

				operator.setLocation(moduleName);

			} else {
				logger.error("The operator is not bound to database object");

				operator.setBoundName(opName);

				operator.setLocation("WAREHOUSE_TARGET");

				logger.debug("Assign default values to bound name and location name ");

				// return;
			}
		}

		MapAttributeGroup[] opGroups = op.getAttributeGroups(Mappable.DIRECTION_ALL);

		if (opGroups != null && opGroups.length > 0) {
			addGroup(operator, opGroups);
		}

		fillOperatorProperty(operator, op);

		if (opType.equalsIgnoreCase("TABLE") || opType.equalsIgnoreCase("VIEW") || opType.equalsIgnoreCase("FLATFILE")) {

			identifySourceOrTarget(operator);
		}

		mapping.addToOperators(operator);

	}

	private boolean fillOperatorProperty(MetadataOperator operator, MapOperator op) {

		boolean processed = false;

		String opType = operator.getType();
		logger.debug("Fill the " + opType + " type operator (" + operator.getName() + ") properties");
		String[] keys;
		String patternProperty;

		MapAttributeGroup[] groups;

		switch (opType) {
			case "LOOKUP":

				// Lookup condition is defined in Group property
				groups = op.getAttributeGroups(Mappable.DIRECTION_OUTPUT);

				for (MapAttributeGroup group : groups) {
					keys = group.getPropertyKeys();
					for (String key : keys) {
						// logger.debug("Get lookup group property " + key);
						//
						// try {
						// Object pValue = group.getPropertyValue(key);
						// if (pValue instanceof String) {
						// logger.debug("...Get String property " + key +
						// " from group " + group.getName() + " :\t"
						// + group.getPropertyValue(key).toString());
						// }
						// } catch (NoSuchPropertyException e) {
						// // by pass
						// }

						if (key.toUpperCase().matches("LOOKUP_CONDITION")) {

							try {
								String value = group.getPropertyValue(key).toString();
								MetadataProperty p;
								p = new MetadataProperty(key, value);
								operator.addToProperties(p);
								logger.debug("Found LOOKUP_CONDITION : " + value);

							} catch (NoSuchPropertyException e) {
								logger.error("Failed to add the property " + key);
								logger.error(HelperBase.getStackTrace(e));
							}

						}

						if (key.toUpperCase().matches("LOOKUP_BOUND_OBJECT")) {
							String pValue = "";

							MetadataProperty p = null;

							try {

								pValue = group.getPropertyValue(key).toString();

								logger.debug("The original value of LOOKUP_BOUND_OBJECT is " + pValue);

								if (pValue == null || pValue.length() == 0) {

									// pValue =
									// group.getPropertyValue("BOUND_NAME").toString();

									OWBNamedObject b = op.getBoundObject();

									if (b != null) {
										p = new MetadataProperty("LOOKUP_OBJECT_NAME", b.getName());

										logger.debug("Failed to find the value of LOOKUP_BOUND_OBJECT. So give it default value as the bound name "
												+ b.getName());
									} else {
										p = new MetadataProperty("LOOKUP_OBJECT_NAME", "Missing");
										logger.debug("Set the value of LOOKUP_OBJECT_NAME to missing");
									}
								} else {
									String[] values = pValue.split(" ");

									if (values != null && values.length == 2) {

										p = new MetadataProperty("LOOKUP_OBJECT_TYPE", values[0]);
										operator.addToProperties(p);
										p = new MetadataProperty("LOOKUP_OBJECT_NAME", values[1]);

										logger.debug("Found LOOKUP_OBJECT_TYPE : " + values[0]
												+ " ,LOOKUP_OBJECT_NAME : " + values[1]);

									} else {
										p = new MetadataProperty("LOOKUP_OBJECT_NAME", pValue);

										logger.debug("Found LOOKUP_OBJECT : " + pValue);
									}
								}

								operator.addToProperties(p);

							} catch (NoSuchPropertyException e) {
								logger.error("Failed to add the property " + key);
								logger.error(HelperBase.getStackTrace(e));
							}
						}

						if (key.toUpperCase().matches("CREATE_NO_MATCH_ROW")) {

							try {
								String value = group.getPropertyValue(key).toString();
								MetadataProperty p;
								p = new MetadataProperty(key, value);
								operator.addToProperties(p);
								logger.debug("Found CREATE_NO_MATCH_ROW : " + value);

							} catch (NoSuchPropertyException e) {
								logger.error("Failed to add the property " + key);
								logger.error(HelperBase.getStackTrace(e));
							}

						}

						// if
						// (key.toUpperCase().matches("DYNAMIC_LOOKUP_FILTER"))
						// {
						//
						// try {
						// String value =
						// group.getPropertyValue(key).toString();
						// MetadataProperty p;
						// p = new MetadataProperty(key, value);
						// operator.addToProperties(p);
						// logger.debug("Found DYNAMIC_LOOKUP_FILTER : " +
						// value);
						//
						// } catch (NoSuchPropertyException e) {
						// logger.error("Failed to add the property " + key);
						// logger.error(HelperBase.getStackTrace(e));
						// }
						//
						// }

					}
				}

				processed = true;

				break;

			case "JOINER":
			case "FILTER":
			case "EXPRESSION":
			case "UNION":

				patternProperty = ".*CONDITION.*";
				fillPatternPropertyForOperator(operator, op, patternProperty);

				processed = true;

				break;

			case "AGGREGATOR":
				// Lookup condition is defined in Group property

				fillPropertyForOperator(operator, op, "GROUP_BY_CLAUSE");
				fillPropertyForOperator(operator, op, "HAVING_CLAUSE");

				// Field "IS_AGGREGATION" : boolean

				processed = true;

				break;

			case "DEDUPLICATOR":
				processed = true;

				break;

			case "EXTERNAL_TABLE":
			case "MATERIALIZED_VIEW":
			case "VIEW":

				fillPropertyForOperator(operator, op, "BOUND_NAME");

			case "TABLE":
				/*
				 * For the target table, add loading_type property. The loading
				 * operation to be performed when this is a target :
				 * CHECK_INSERT, DELETE, DELETE_INSERT, DERIVE_FROM_LCR, INSERT,
				 * INSERT_UPDATE, NONE, TRUNCATE_INSERT, UPDATE, UPDATE_INSERT
				 */
				patternProperty = ".*LOADING_TYPE.*";
				fillPatternPropertyForOperator(operator, op, patternProperty);

				processed = true;

				break;

			case "FLAT_FILE":

				fillPropertyForOperator(operator, op, "BOUND_NAME");
				fillPropertyForOperator(operator, op, "ESCAPE_CHARACTER_FOR_ENCLOSURE");
				fillPropertyForOperator(operator, op, "FIELD_ENCLOSURE_CHARACTERS");
				fillPropertyForOperator(operator, op, "FIELD_NAMES_IN_THE_FIRST_ROW");
				fillPropertyForOperator(operator, op, "FIELD_TERMINATION_CHARACTER");
				fillPropertyForOperator(operator, op, "FILE_FORMAT");
				fillPropertyForOperator(operator, op, "RECORDS_TO_SKIP");
				fillPropertyForOperator(operator, op, "RECORD_DELIMITER");
				fillPropertyForOperator(operator, op, "RECORD_SIZE");
				fillPropertyForOperator(operator, op, "SAMPLED_FILE_NAME");

				processed = true;

				break;

			case "SET":

				// values : INTERSECT, MINUS, UNION, UNIONALL
				fillPropertyForOperator(operator, op, "SET_OPERATION");

				processed = true;

				break;

			case "PREMAPPING_PROCESS":
			case "POSTMAPPING_PROCESS":

				// Field "FUNCTION_RETURN" : Specifies whether this output is
				// the return value of this function

				fillPropertyForOperator(operator, op, "FUNCTION_NAME");

				processed = true;

				break;

			case "TRANSFORMATION":

				fillPropertyForOperator(operator, op, "BOUND_NAME");
				fillPropertyForOperator(operator, op, "FUNCTION_NAME");
				fillPropertyForOperator(operator, op, "RETURN_TYPE");

				processed = true;

				break;

			case "TABLE_FUNCTION":
				fillPropertyForOperator(operator, op, "TABLE_FUNCTION_NAME");

				processed = true;

				break;
			case "SPLITTER":

				fillPropertyForGroup(operator, op, "SPLIT_CONDITION");
				processed = true;

				break;

			case "SORTER":

				fillPropertyForOperator(operator, op, "ORDER_BY_CLAUSE");
				processed = true;

				break;

			case "UNPIVOT":
				// Field "GROUP_KEY" : A boolean value to indicate whether this
				// input attribute is a part of the unpivot group key.

				// Field "UNPIVOT_EXPRESSION" : String , An expression that
				// gives the input attribute to be used as the output of this
				// attribute.

				fillPropertyForGroup(operator, op, "ROW_LOCATOR_VALUES");
				fillPropertyForGroup(operator, op, "ROW_LOCATOR");

				fillPropertyForField(operator, op, "GROUP_KEY");
				fillPropertyForField(operator, op, "UNPIVOT_EXPRESSION");
				fillPropertyForField(operator, op, "MATCHING_ROW");

				processed = true;

				break;

			case "PIVOT":

				// Field "PIVOT_EXPRESSION" : String, A comma-separated
				// expression that gives the input attribute to be used for each
				// output row in the pivot group.

				fillPropertyForField(operator, op, "PIVOT_EXPRESSION");
				processed = true;

				break;
			default:
				logger.debug("Don't need to care of the " + opType + " type operator");

				// To process all the operators
				processed = true;
		}

		return processed;
	}

	private void fillPropertyForField(MetadataOperator operator, MapOperator op, String propName) throws OWBException {
		HelperBase.printSplitLine(logger);
		String value = null;
		String location = null;
		String[] keys;
		MapAttributeGroup[] groups;
		groups = op.getAttributeGroups(Mappable.DIRECTION_ALL);
		logger.trace("--- Find property (" + propName + ") for Fields ");
		logger.trace("To find the property " + propName + " for fields in operator " + op.getName());
		for (MapAttributeGroup group : groups) {

			MapAttribute[] fields = group.getAttributes();
			for (MapAttribute field : fields) {

				boolean foundProperty = false;

				keys = field.getPropertyKeys();
				for (String key : keys) {
					logger.trace("\t\t\tGet group (" + group.getName() + ") field (" + field.getName()
							+ ") property : " + key);
					if (key.toUpperCase().matches(propName)) {

						try {
							value = field.getPropertyValue(key).toString();

							if (value != null) {

								location = group.getName() + "." + field.getName();

								foundProperty = true;
							}
						} catch (NoSuchPropertyException | NullPointerException e) {
							logger.error("Failed to add the property " + key);
							logger.error(HelperBase.getStackTrace(e));
						}

					}

				}

				if (!foundProperty) {
					try {
						logger.trace("- Check for specified property (" + propName + ") : "
								+ field.getPropertyValueString(propName));
						value = field.getPropertyValueString(propName);
						location = group.getName() + "." + field.getName();

						foundProperty = true;

					} catch (NoSuchPropertyException | TypeMismatchException e) {
						logger.error("Faild to get property (" + propName + ") using method getPropertyValueString");
					}
				}

				if (foundProperty) {
					MetadataProperty p;
					p = new MetadataProperty(propName, value);

					p.setLocation(location);

					MetadataGroup mGroup = operator.getGroupByName(group.getName());
					MetadataField mField = mGroup.getFieldByName(field.getName());

					mField.addToProperties(p);
					logger.trace("*Found " + propName + " : " + value);
				}

			}
		}

		HelperBase.printSplitLine(logger);
	}

	private void fillPropertyForGroup(MetadataOperator operator, MapOperator op, String propName) throws OWBException {
		HelperBase.printSplitLine(logger);
		boolean foundProperty = false;
		String value = null;
		String location = null;
		String[] keys;
		MapAttributeGroup[] groups;
		groups = op.getAttributeGroups(Mappable.DIRECTION_ALL);
		logger.trace("--- Find property (" + propName + ") for groups ");
		for (MapAttributeGroup group : groups) {

			keys = group.getPropertyKeys();

			logger.trace("- Check for all the properties on group " + group.getName());
			for (String key : keys) {
				logger.trace("\t\tGet group (" + group.getName() + ") property : " + key);
				if (key.toUpperCase().matches(propName)) {

					try {
						value = group.getPropertyValue(key).toString();
						location = group.getName();
						foundProperty = true;
					} catch (NoSuchPropertyException | NullPointerException e) {
						logger.error("Failed to add the property " + key);
						logger.error(HelperBase.getStackTrace(e));
					}

				}

			}

			if (!foundProperty) {
				try {
					logger.trace("- Check for specified property (" + propName + ") : "
							+ group.getPropertyValueString(propName));
					value = group.getPropertyValueString(propName);
					location = group.getName();

					foundProperty = true;

				} catch (NoSuchPropertyException | TypeMismatchException e) {
					logger.error("Faild to get property (" + propName + ") using method getPropertyValueString");
				}
			}

			if (foundProperty) {
				MetadataProperty p;
				p = new MetadataProperty(propName, value);

				p.setLocation(location);

				MetadataGroup mGroup = operator.getGroupByName(group.getName());

				mGroup.addToProperties(p);

				logger.trace("*Found " + propName + " : " + value);

			}

		}

		if (foundProperty) {
			logger.debug("The group property " + propName + " is found !");
		} else {
			logger.debug("The group property " + propName + " isn't found !");
		}

		HelperBase.printSplitLine(logger);
	}

	private void fillPropertyForOperator(MetadataOperator operator, MapOperator op, String propName) {
		HelperBase.printSplitLine(logger);
		logger.trace("--- Find property (" + propName + ") for operator ");

		String[] keys;
		keys = op.getPropertyKeys();
		for (String key : keys) {
			if (key.toUpperCase().matches(propName)) {

				try {
					String value = op.getPropertyValue(key).toString();
					MetadataProperty p;
					p = new MetadataProperty(key, value);
					operator.addToProperties(p);
					logger.trace("\tFound " + propName + " : " + value);

				} catch (NoSuchPropertyException | NullPointerException e) {
					logger.error("Failed to add the property " + key);
					logger.error(HelperBase.getStackTrace(e));
				}

			}
		}
		HelperBase.printSplitLine(logger);
	}

	private void fillPatternPropertyForOperator(MetadataOperator operator, MapOperator op, String propertyPattern) {
		HelperBase.printSplitLine(logger);
		logger.trace("--- Find patttern property (" + propertyPattern + ") for operator ");

		String[] keys;
		keys = op.getPropertyKeys();

		for (String key : keys) {

			// logger.debug("...Get property " + key);
			if (key.toUpperCase().matches(propertyPattern)) {

				logger.trace("\tFound condition property " + key);
				try {
					MetadataProperty p;
					p = new MetadataProperty(key, op.getPropertyValue(key).toString());
					operator.addToProperties(p);
				} catch (NoSuchPropertyException | NullPointerException e) {
					logger.error("Failed to add the property " + key);
					logger.error(HelperBase.getStackTrace(e));
				}

			}
		}
		HelperBase.printSplitLine(logger);
	}

	private void addGroup(MetadataOperator operator, MapAttributeGroup[] opGroups) throws IllegalArgumentException,
			IOException {

		if (opGroups != null && opGroups.length > 0) {
			for (MapAttributeGroup owbMapGroup : opGroups) {

				String groupName = owbMapGroup.getName();
				String groupDirection = getGroupDirection(owbMapGroup);
				String uoid = owbMapGroup.getUOID();

				MetadataGroup group = new MetadataGroup(groupName, groupDirection);
				group.setUoid(uoid);

				String opType = operator.getType();
				String opName = operator.getName();

				logger.debug("Start to process the group for operator " + opName + "(" + opType + ")");
				if (opType.equalsIgnoreCase("TABLE") || opType.equalsIgnoreCase("VIEW")) {

					OracleHelper db = new OracleHelper();

					HashMap<String, MetadataField> fieldsMap = getDBTable(db, operator);

					if (fieldsMap != null && fieldsMap.size() > 0) {

						String boundName = operator.getBoundName();
						String bizName = operator.getBusiness_name();
						logger.debug("Found " + fieldsMap.values().size() + " fields for table " + bizName
								+ "( bound : " + boundName + ") for operator " + opName);

						addFieldsAttributes(fieldsMap, owbMapGroup);

						ArrayList<MetadataField> fields = new ArrayList<MetadataField>();

						fields.addAll(fieldsMap.values());

						group.setFields(fields);
					} else {
						logger.error("No field found");
					}

				} else {
					addFieldsAttributes(group, owbMapGroup);
				}

				sortByPosition(group.getFields());

				operator.addToGroups(group);

			}
		}
	}

	private HashMap<String, MetadataField> getDBTable(OracleHelper db, MetadataOperator operator) throws IOException {
		HashMap<String, MetadataField> fieldsMap = new HashMap<String, MetadataField>();
		String opBoundName = operator.getBoundName();

		try {

			fieldsMap = db.getTableColumns(opBoundName);
		} catch (Exception e) {
			String bizName = operator.getBusiness_name();
			logger.debug("Can't find the table using bound name" + opBoundName
					+ ". Try to get table using business name : " + bizName);

			try {
				fieldsMap = db.getTableColumns(bizName);

				operator.setBoundName(bizName);
				logger.debug("Correct the bound name with the business name : " + bizName);

			} catch (SQLException se) {
				logger.error("Can't find the table using business name" + bizName);
			}

			logger.error(HelperBase.getStackTrace(e));

		}
		return fieldsMap;
	}

	private void addFieldsAttributes(HashMap<String, MetadataField> hashMap, MapAttributeGroup owbMapGroup) {
		MapAttribute[] attributes = owbMapGroup.getAttributes();
		if (attributes != null) {
			for (MapAttribute attr : attributes) {
				try {
					String fieldName = attr.getPropertyValueString("NAME");
					logger.debug("Start to find field [" + fieldName + "]");
					MetadataField field = hashMap.get(fieldName);

					logger.debug("Start to fill attributes for field [" + field.getName() + "]");
					fillField(attr, field);
				} catch (NoSuchPropertyException | TypeMismatchException | NullPointerException e) {
					logger.error("Fail to get field properties");
					logger.error(HelperBase.getStackTrace(e));
				}

			}
		}

	}

	private void addFieldsAttributes(MetadataGroup group, MapAttributeGroup owbMapGroup) {

		MapAttribute[] attributes = owbMapGroup.getAttributes();
		ArrayList<MetadataField> fields = new ArrayList<MetadataField>();
		if (attributes != null) {
			for (MapAttribute attr : attributes) {

				try {
					String fieldName = attr.getPropertyValueString("NAME");
					logger.debug("Start to find field [" + fieldName + "]");

					MetadataField field = new MetadataField();
					field.setName(fieldName);
					field.setNullable(true);

					fillField(attr, field);
					fields.add(field);

				} catch (NoSuchPropertyException | TypeMismatchException | NullPointerException e) {
					logger.error("Fail to get field properties");
					logger.error(HelperBase.getStackTrace(e));

					continue;
				}

			}

			logger.debug("There are " + fields.size() + " fields in the group " + group.getName());
			group.setFields(fields);
		}
	}

	private void fillField(MapAttribute attr, MetadataField field) throws NoSuchPropertyException,
			TypeMismatchException {
		field.setUoid(attr.getUOID());
		field.setPosition(attr.getPropertyValueString("POSITION"));
		field.setBound_name(attr.getPropertyValueString("BOUND_NAME"));
		field.setBusiness_name(attr.getPropertyValueString("LOGICAL_NAME"));
		field.setDescription(attr.getPropertyValueString("DESCRIPTION"));

		try {
			String exp = attr.getPropertyValueString("EXPRESSION");
			if (!exp.startsWith("&amp")) {
				field.setExpression(exp);
			}

			// String defaultValue =
			// attr.getPropertyValueString("DEFAULT_VALUE");
			// if (defaultValue != null) {
			// field.setDefaultValue(defaultValue);
			// }

			String dataType = attr.getPropertyValueString("DATA_TYPE");
			field.setData_type(dataType);

			if (dataType.equals("VARCHAR")) {
				field.setPrecision(attr.getPropertyValueString("LENGTH"));

			} else if (dataType.equals("NUMERIC")) {
				field.setPrecision(attr.getPropertyValueString("LENGTH"));
				field.setScale(attr.getPropertyValueString("SCALE"));
			}
			Mappable[] outputConnections = attr.getConnectedMappables(Mappable.MAPPABLE_TYPE_ATTRIBUTE,
					Mappable.DIRECTION_OUTPUT);

			if (outputConnections != null && outputConnections.length > 0) {
				addConnections(field, outputConnections, Mappable.DIRECTION_OUTPUT);
			}

			Mappable[] inputConnections = attr.getConnectedMappables(Mappable.MAPPABLE_TYPE_ATTRIBUTE,
					Mappable.DIRECTION_INPUT);

			if (inputConnections != null && inputConnections.length > 0) {
				addConnections(field, inputConnections, Mappable.DIRECTION_INPUT);
			}

			if (attr.getPropertyValueString("MATCH_COLUMN_WHEN_UPDATING_ROW").equals("NO")) {
				field.setBizKey(false);
			} else {
				field.setBizKey(true);
			}

			if (attr.getPropertyValueString("LOAD_COLUMN_WHEN_UPDATING_ROW").equals("NO")) {
				field.setUsedWhenUpdate(false);
			} else {
				field.setUsedWhenUpdate(true);
			}
		} catch (NoSuchPropertyException | NullPointerException e) {
			// bypass
		}
	}

	private void addConnections(MetadataField field, Mappable[] connections, int directionOutput) {

		for (Mappable connection : connections) {

			if (directionOutput == Mappable.DIRECTION_OUTPUT) {
				MapOperatorImpl targetOP = (MapOperatorImpl) connection.getParentMappable().getParentMappable();
				MapAttributeGroupImpl targetGroup = (MapAttributeGroupImpl) connection.getParentMappable();
				MapAttributeImpl targetAttr = (MapAttributeImpl) connection;

				MetadataConnection mapConnection = new MetadataConnection(field.getUoid(), targetAttr.getUOID());

				mapConnection.setTargetOperator(targetOP.getName());
				mapConnection.setTargetGroup(targetGroup.getName());
				mapConnection.setTargetField(targetAttr.getName());

				field.addToOutputConnections(mapConnection);
			} else if (directionOutput == Mappable.DIRECTION_INPUT) {

				MapOperatorImpl sourceOP = (MapOperatorImpl) connection.getParentMappable().getParentMappable();
				MapAttributeGroupImpl sourceGroup = (MapAttributeGroupImpl) connection.getParentMappable();
				MapAttributeImpl sourceAttr = (MapAttributeImpl) connection;

				MetadataConnection mapConnection = new MetadataConnection(sourceAttr.getUOID(), field.getUoid());

				//Consolidate the same source name
				String opType = sourceOP.getOperatorType();

				if (OWBPropConstants.isSupportedOWBSourceOperator(opType)) {
					OWBNamedObject boundObject = sourceOP.getBoundObject();

					if (boundObject != null) {
						String boundName = boundObject.getName();
						mapConnection.setSourceOperator(boundName);
					} else {
						mapConnection.setSourceOperator(sourceOP.getName());
					}
				} else {
					mapConnection.setSourceOperator(sourceOP.getName());
				}
				mapConnection.setSourceGroup(sourceGroup.getName());
				mapConnection.setSourceField(sourceAttr.getName());

				field.addToInputConnections(mapConnection);
			}
		}
	}

	private void identifySourceOrTarget(MetadataOperator operator) {

		boolean hasInputConn = false;
		boolean hasOutputConn = false;

		ArrayList<MetadataGroup> groups = operator.getGroups();

		for (MetadataGroup group : groups) {
			ArrayList<MetadataField> fields = group.getFields();

			for (MetadataField field : fields) {

				if (field.getInputConnections().size() > 0) {
					hasInputConn = true;
				}

				if (field.getOutputConnections().size() > 0) {
					hasOutputConn = true;
				}
			}
		}

		if (!hasOutputConn) {
			operator.setTarget(true);
		}

		if (!hasInputConn) {
			operator.setSource(true);
		}
	}

	private String getGroupDirection(MapAttributeGroup group) {
		String directionString;
		switch (group.getDirection()) {
			case MapAttributeGroup.DIRECTION_INPUT_OUTPUT:
				directionString = "INOUTGRP";
				break;
			case MapAttributeGroup.DIRECTION_INPUT:
				directionString = "INGRP";
				break;
			case MapAttributeGroup.DIRECTION_OUTPUT:
				directionString = "OUTGRP";
				break;
			default:
				directionString = "ALL";
				break;
		}

		return directionString;
	}

	private void sortByType(ArrayList<MetadataOperator> operators) {
		Collections.sort(operators, new Comparator<MetadataOperator>() {

			public int compare(MetadataOperator op, MetadataOperator otherOP) {
				int opRank = 0;
				int otherOPRank = 0;

				opRank = (op.isSource() ? 1 : 2) + (op.isTarget() ? 3 : 2);
				otherOPRank = (otherOP.isSource() ? 1 : 2) + (otherOP.isTarget() ? 3 : 2);

				// logger.debug("Comparing the rank of " + op.getBusiness_name()
				// + " to " + otherOP.getBusiness_name());
				// logger.debug(op.getBusiness_name() + " rank : " + opRank);
				// logger.debug(otherOP.getBusiness_name() + " rank : " +
				// otherOPRank);
				return opRank > otherOPRank ? 1 : (opRank < otherOPRank ? -1 : 0);
			}
		});
	}

	private void sortByPosition(ArrayList<MetadataField> fields) {
		Collections.sort(fields, new Comparator<MetadataField>() {

			public int compare(MetadataField field, MetadataField otherField) {

				if (field != null && otherField != null && field.getPosition() != null
						&& otherField.getPosition() != null) {
					return field.getPosition().compareTo(otherField.getPosition());
				} else {
					return 0;
				}
			}
		});
	}

	public String getXmlOutputPath() {
		return xmlOutputPath;
	}

	public void setXmlOutputPath(String xmlOutputPath) {
		this.xmlOutputPath = xmlOutputPath;
	}
}
