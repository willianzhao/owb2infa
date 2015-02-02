package org.willian.owb2infa.helper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HelperBase {

	private Properties properties;
	protected String CONF_FILE;

	private static Logger logger = LogManager.getLogger(HelperBase.class);

	public static String getStackTrace(Exception e) {
		StringWriter sWriter = new StringWriter();
		PrintWriter pWriter = new PrintWriter(sWriter);
		e.printStackTrace(pWriter);
		return sWriter.toString();
	}

	public Properties getLocalProps(String configFile) throws IOException, IllegalArgumentException {

		if (configFile == null) {
			throw new IllegalArgumentException("Configure file is not provided");
		}

		if (properties != null && properties.size() > 0) {
			return properties;
		} else {
			properties = new Properties();

			InputStream propStream = getClass().getClassLoader().getResourceAsStream(configFile);

			if (propStream != null) {
				properties.load(propStream);
				return properties;
			} else {
				throw new IOException(configFile
						+ "file not found.Add Directory containing pcconfig.properties to ClassPath");
			}
		}
	}

	public static PrintWriter getPrintWriter(String dirName, String fileName) throws IOException {
		File dir = new File(dirName);
		if (!dir.exists()) {
			dir.mkdirs();
			logger.debug("Create folder " + dirName);
		}
		File file = new File(dir, fileName);

		return new PrintWriter(new FileOutputStream(file), true);
	}

	public static InputStream getInputStream(String dirName, String fileName) throws IOException {
		File dir = new File(dirName);
		if (!dir.exists()) {
			dir.mkdirs();
			logger.debug("Create folder " + dirName);
		}
		File file = new File(dir, fileName);
		return new FileInputStream(file);
	}

	public static LineNumberReader getLineNumberReader(String dirName, String fileName) throws IOException {
		File dir = new File(dirName);
		FileReader fr = null;
		LineNumberReader lnr = null;

		if (!dir.exists()) {
			dir.mkdirs();
			logger.debug("Create folder " + dirName);
		}
		File file = new File(dir, fileName);
		fr = new FileReader(file);
		lnr = new LineNumberReader(fr);

		return lnr;
	}

	public static void printSplitLine(Logger logger) {
		logger.debug(HelperBase.repeat("=", 60));
	}

	public static final String repeat(String str, int times) {
		if (str == null)
			return null;

		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < times; i++) {
			sb.append(str);
		}
		return sb.toString();
	}

}
