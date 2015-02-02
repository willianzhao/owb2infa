package org.willian.owb2infa.helper.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataField;

public class OracleHelper extends HelperBase {

	Properties properties;

	private static Connection con;

	private Logger logger = LogManager.getLogger(OracleHelper.class);

	public OracleHelper() throws IllegalArgumentException, IOException {
		this.CONF_FILE = "oracle.properties";
		properties = this.getLocalProps(CONF_FILE);
	}

	public OracleHelper(String conf) throws IllegalArgumentException, IOException {
		this.CONF_FILE = conf;
		properties = this.getLocalProps(CONF_FILE);

		logger.debug("Set the oracle configure file to " + conf);
	}

	public HashMap<String, MetadataField> getTableColumns(String table) throws SQLException {

		HashMap<String, MetadataField> fieldsMap = new HashMap<String, MetadataField>();

		String user = properties.getProperty("USER").toUpperCase();

		DatabaseMetaData meta;
		try {
			meta = getDBConnectionInstance().getMetaData();

			logger.debug("Get DatabaseMetaData object under user " + user);
			// fillColumns(table, fieldsMap, user, meta);
			// fillColumns(table, fieldsMap);
			fillColumns(table, fieldsMap, user);

			logger.debug("Retrieved " + fieldsMap.size() + " columns");
			try {
				checkPrimaryKey(table, fieldsMap, user, meta);

				logger.debug("Finish check the primary key");
			} catch (SQLException e) {
				logger.error("Failed to get the primary key from " + table);
				logger.error(e.getLocalizedMessage());
			}

		} catch (SQLException e) {
			logger.debug("Failed to get the Oracle metadata object under user " + user);

			user = properties.getProperty("TRYSCHEMA").toUpperCase();
			try {
				meta = getDBConnectionInstance().getMetaData();

				logger.debug("Get DatabaseMetaData object under user " + user);
				// fillColumns(table, fieldsMap, user, meta);
				// fillColumns(table, fieldsMap);
				fillColumns(table, fieldsMap, user);

				logger.debug("Retrieved " + fieldsMap.size() + " columns");
				try {
					checkPrimaryKey(table, fieldsMap, user, meta);

					logger.debug("Finish check the primary key");
				} catch (SQLException innerException) {
					logger.error("Failed to get the primary key from " + table + " under user " + user);
					logger.error(innerException.getLocalizedMessage());
				}
			} catch (SQLException innerException2) {
				logger.error("Failed to get the Oracle metadata object under user " + user);
				logger.error(innerException2.getLocalizedMessage());
				throw innerException2;
			}
		}

		logger.debug("fieldsMap contains " + fieldsMap.size() + " elements");
		return fieldsMap;

	}

	private void fillColumns(String table, HashMap<String, MetadataField> fieldsMap, String user, DatabaseMetaData meta)
			throws SQLException {

		logger.debug("Start to get columns of " + user + "." + table + " from Oracle db");
		ResultSet columns = meta.getColumns(null, user, table, null);

		while (columns.next()) {

			MetadataField column = new MetadataField();
			String columnName = columns.getString("COLUMN_NAME");

			logger.debug("process column " + columnName);
			column.setName(columnName);
			column.setDbColumnType(columns.getString("TYPE_NAME"));
			column.setDbColumnPrecision(columns.getString("COLUMN_SIZE"));
			column.setDbColumnScale(columns.getString("DECIMAL_DIGITS"));
			boolean nullable = columns.getString("IS_NULLABLE").equalsIgnoreCase("YES") ? true : false;
			column.setNullable(nullable);

			logger.debug("Add column " + columnName + " of table " + table);

			fieldsMap.put(columnName, column);
		}
	}

	private void fillColumns(String table, HashMap<String, MetadataField> fieldsMap, String user) throws SQLException {
		logger.debug("Start to get columns of " + user + "." + table + " from Oracle db");

		final String sqlBase = "select * from ";
		String sql = sqlBase + user + "." + table + " where 1 = 2";
		Statement stmt = null;
		ResultSet rslt = null;
		try {
			stmt = getDBConnectionInstance().createStatement();
			rslt = stmt.executeQuery(sql);
			ResultSetMetaData meta = rslt.getMetaData();
			int numCols = meta.getColumnCount();
			for (int i = 1; i <= numCols; i++) {
				MetadataField column = new MetadataField();

				String columnName = meta.getColumnName(i);

				column.setName(columnName);
				column.setDbColumnType(meta.getColumnTypeName(i));

				column.setDbColumnPrecision(Integer.toString(meta.getPrecision(i)));
				column.setDbColumnScale(Integer.toString(meta.getScale(i)));
				boolean nullable = meta.isNullable(i) == ResultSetMetaData.columnNoNulls ? true : false;
				column.setNullable(nullable);

				logger.debug("Add column " + columnName + " of table " + user + "." + table);

				fieldsMap.put(columnName, column);

			}
		} catch (SQLException e) {
			logger.error("Failed to get oracle table " + table + " metadata");
			logger.error(HelperBase.getStackTrace(e));
			throw e;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ignore) {
				}
			}
			if (rslt != null) {
				try {
					rslt.close();
				} catch (SQLException ignore) {
				}
			}
		}
	}

	private void fillColumns(String table, HashMap<String, MetadataField> fieldsMap) throws SQLException {

		logger.debug("Start to get columns of " + table + " from Oracle db");

		final String sqlBase = "select * from ";
		String sql = sqlBase + table + " where 1 = 2";
		Statement stmt = null;
		ResultSet rslt = null;
		try {
			stmt = getDBConnectionInstance().createStatement();
			rslt = stmt.executeQuery(sql);
			ResultSetMetaData meta = rslt.getMetaData();
			int numCols = meta.getColumnCount();
			for (int i = 1; i <= numCols; i++) {
				MetadataField column = new MetadataField();

				String columnName = meta.getColumnName(i);

				column.setName(columnName);
				column.setDbColumnType(meta.getColumnTypeName(i));

				column.setDbColumnPrecision(Integer.toString(meta.getPrecision(i)));
				column.setDbColumnScale(Integer.toString(meta.getScale(i)));
				boolean nullable = meta.isNullable(i) == ResultSetMetaData.columnNoNulls ? true : false;
				column.setNullable(nullable);

				logger.debug("Add column " + columnName + " of table " + table);

				fieldsMap.put(columnName, column);

			}
		} catch (SQLException e) {
			logger.error("Failed to get oracle table " + table + " metadata");
			logger.error(HelperBase.getStackTrace(e));
			throw e;
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException ignore) {
				}
			}
			if (rslt != null) {
				try {
					rslt.close();
				} catch (SQLException ignore) {
				}
			}
		}

	}

	private void checkPrimaryKey(String table, HashMap<String, MetadataField> fieldsMap, String user,
			DatabaseMetaData meta) throws SQLException {

		logger.debug("Try to find the primary key in table " + table);
		ResultSet primaryKeys = meta.getPrimaryKeys(null, user, table);

		boolean foundPK = false;

		while (primaryKeys.next()) {

			String columnName = primaryKeys.getString("COLUMN_NAME");
			if (fieldsMap.get(columnName) != null) {
				fieldsMap.get(columnName).markAsKey(true);

				foundPK = true;
				logger.debug("Add primary column " + columnName + " in " + table);
			} else {
				logger.error("Failed to find the column " + columnName + " in fieldsMap structure");
			}
		}

		if (!foundPK) {
			checkUniqueKey(table, fieldsMap, user, meta);
		}
	}

	private void checkUniqueKey(String table, HashMap<String, MetadataField> fieldsMap, String user,
			DatabaseMetaData meta) throws SQLException {
		logger.debug("Try to find the unique key in table " + table);
		boolean listUniqueIndex = true;
		ResultSet uniqueKeys = meta.getIndexInfo(null, user, table, listUniqueIndex, true);
		while (uniqueKeys.next()) {
			String indexName = uniqueKeys.getString("INDEX_NAME");
			if (indexName == null) {
				continue;
			}
			String columnName = uniqueKeys.getString("COLUMN_NAME");
			fieldsMap.get(columnName).markAsKey(true);
		}
	}

	private Connection getDBConnectionInstance() throws SQLException {

		if (con == null) {
			String host = properties.getProperty("HOST");
			String port = properties.getProperty("PORT");
			String serviceName = properties.getProperty("SERVICENAME");
			String user = properties.getProperty("USER");
			String password = properties.getProperty("PASSWORD");

			// jdbc:oracle:thin:@//oracle.hostserver2.mydomain.ca:1522/ABCD
			String conString = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName;

			logger.debug("Get oracle connection string as " + conString);
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			con = DriverManager.getConnection(conString, user, password);
			logger.debug("Construct Oracle Connection object");
		}

		return con;
	}

}
