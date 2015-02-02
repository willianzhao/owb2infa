package org.willian.owb2infa.helper.infa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.willian.owb2infa.model.MetadataConnection;
import org.willian.owb2infa.model.MetadataField;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;
import org.willian.owb2infa.model.MetadataProperty;

public class SQLProcessor {

	private static Logger logger = LogManager.getLogger(SQLProcessor.class);

	public static String generateSourceFilter() {
		StringBuffer sourceFilter = new StringBuffer();

		// TODO: extract the date field from condition
		String leftDateField = "LAST_UPDATED_DATE";

		sourceFilter.append("( ");

		sourceFilter
				.append(leftDateField
						+ " >= to_date('$$read_src_data_from_date', 'mm/dd/yyyy hh24:mi:ss') ");

		sourceFilter
				.append("\n -- Please change the source filter according to business logic\n");
		// TODO: extract the date field from condition
		String rightDateField = null;
		if (rightDateField != null) {
			sourceFilter
					.append(" and "
							+ rightDateField
							+ " < to_date('$$read_src_data_end_date', 'mm/dd/yyyy hh24:mi:ss') ");
		}

		sourceFilter.append(")");

		String filterStatement = sourceFilter.toString();

		logger.trace("Generate the source filter : " + filterStatement);

		return filterStatement;
	}

	public static String removeLoadControlClause(MetadataMapping owbMapping,
			String condition) {

		// TODO: Impl
		String dealtResult = condition;

		return dealtResult;
	}

	public static String getOperatorProperty(MetadataOperator op,
			String propertyName) {

		String value = null;

		List<MetadataProperty> properties = op.getProperties();
		for (MetadataProperty property : properties) {
			String name = property.getName();
			if (name.equals(propertyName)) {

				value = property.getValue();

				logger.trace("Got operator property " + name + " : " + value);
			}
		}

		return value;

	}

	public static String fetchAndFormatCondition(MetadataMapping owbMapping,
			MetadataOperator op, HashMap<String, String> replacedClause) {
		String formattedCondition = null;

		if (op.getType().equals("JOINER")) {

			formattedCondition = formatJoinCondition(owbMapping, op);
		} else {

			List<MetadataProperty> properties = op.getProperties();
			for (MetadataProperty property : properties) {
				if (property.getName().matches(".*CONDITION.*")) {

					if (replacedClause == null) {
						formattedCondition = property.getValue();
					} else {
						formattedCondition = replaceClause(property.getValue(),
								replacedClause);
					}

				}
			}
		}
		return formattedCondition;
	}

	public static String cleanExpression(String expression) {

		HashMap<String, String> replacedClause = new HashMap<String, String>();

		replacedClause.put("/*", "--");
		replacedClause.put("*/", "\n");
		replacedClause.put("SYSDATE()", "SYSDATE");

		String expression2 = replaceClause(expression, replacedClause);

		return expression2;
	}

	public static String replaceClause(String condition,
			HashMap<String, String> replacedClause) {

		String resultCondition = condition;

		logger.trace("Before clause replacement, the origian string is : "
				+ resultCondition);

		for (String key : replacedClause.keySet()) {
			String clause = replacedClause.get(key);

			logger.trace("To replace the key (" + key
					+ ") with string clause (" + clause + ")");
			resultCondition = resultCondition.replace(key, clause);
		}

		logger.trace("After clause replacement, the result is : "
				+ resultCondition);
		return resultCondition;
	}

	private static String formatJoinCondition(MetadataMapping owbMapping,
			MetadataOperator op) {

		String joinCondition = null;

		List<MetadataProperty> properties = op.getProperties();
		for (MetadataProperty property : properties) {
			if (property.getName().equals("JOIN_CONDITION")) {

				joinCondition = property.getValue();

				logger.trace("Ready to format join condition : "
						+ joinCondition);
			}
		}

		// parse the group and replace the group name with the table name in the
		// condition string

		ArrayList<MetadataGroup> groups = op.getGroups();
		String groupName;

		for (MetadataGroup group : groups) {
			if (group.getDirection().matches(".*IN.*")) {
				groupName = group.getName();

				ArrayList<MetadataField> fields = group.getFields();
				for (MetadataField field : fields) {
					ArrayList<MetadataConnection> inputConnections = field
							.getInputConnections();
					if (inputConnections != null) {
						MetadataConnection conn = inputConnections.get(0);

						String inCommingOP = conn.getSourceOperator();

						MetadataOperator inOP = owbMapping
								.getOperatorByName(inCommingOP);

						if (inOP.isSource()) {
							joinCondition = joinCondition.replace(groupName,
									inCommingOP);
							logger.trace("Replace '" + groupName + "' with '"
									+ inCommingOP + "' in join condition");

							break;
						} else {

							logger.trace("Join operator is connected to "
									+ inCommingOP
									+ " which is an operator. Skip the replacement.");
						}
					}
				}
			}
		}
		logger.trace("The formatted join condition is : " + joinCondition);

		return joinCondition;
	}

	public static String convertToSqlNativeType(MetadataField mField) {
		String sqlNativeType = "varchar2";

		String dataType = mField.getData_type();
		String precision = mField.getPrecision();
		String scale = mField.getScale();

		// TODO: Impl
		switch (dataType) {
		case "DATE":
		case "DATETIME":
		case "TIMESTAMP":

			sqlNativeType = "date";
			break;

		case "VARCHAR2":
		case "CHAR":
		case "VARCHAR":
			sqlNativeType = "varchar2";
			break;

		case "DECIMAL":
		case "NUMBER":
		case "NUMERIC":
		case "INT":
			sqlNativeType = "decimal (" + precision + "," + scale + ")";
			break;

		default:
			sqlNativeType = "char";
		}
		return sqlNativeType;
	}

}
