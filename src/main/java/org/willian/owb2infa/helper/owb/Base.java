package org.willian.owb2infa.helper.owb;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.willian.owb2infa.helper.HelperBase;

import oracle.owb.connection.ConnectionFailureException;
import oracle.owb.connection.OWBConnection;
import oracle.owb.connection.RepositoryManager;
import oracle.owb.oracle.OracleModule;
import oracle.owb.project.NoSuchProjectException;
import oracle.owb.project.Project;
import oracle.owb.project.ProjectManager;

public abstract class Base extends HelperBase {

	protected static RepositoryManager reposManager;
	protected static OWBConnection connection;
	protected static ProjectManager projectManager;
	protected static Project project;

	Properties properties;

	private Logger logger = LogManager.getLogger(Base.class);

	Base() throws IllegalArgumentException, IOException {
		CONF_FILE = "owbconfig.properties";
		properties = this.getLocalProps(CONF_FILE);
	}

	public Project getOWBMainProject() {
		String projectName;

		projectName = properties.getProperty(OWBPropConstants.OWB_PROJECT);

		if (projectName == null) {
			logger.error("OWB default project is not set in the configure file.Program terminates.");
			System.exit(1);

		}

		if (project == null) {
			return getOWBProject(projectName);
		} else {
			return project;
		}
	}

	public Project getOWBProject(String projectName) {
		initialConnection();

		try {
			if (projectManager == null) {
				projectManager = ProjectManager.getInstance();
				logger.debug("Get OWB projectManager");
			}

			if (project == null) {
				logger.debug("Get OWB project : " + projectName);
				project = projectManager.setWorkingProject(projectName);
			}
		} catch (NoSuchProjectException e) {
			logger.error("OWB Project " + projectName + " is not found.");
			gracefulExit();
		}
		return project;
	}

	public OracleModule getOracleModuleByName(String module_name) {
		OracleModule module;
		module = getOWBMainProject().findOracleModule(module_name);

		if (module != null) {
			logger.debug("Get OWB module " + module_name);
		} else {
			logger.error("Fail to find the OWB module " + module_name);
		}
		return module;
	}

	public OracleModule getDefaultOracleModule() {
		String defaultModule = properties.getProperty(OWBPropConstants.OWB_MODULE);
		return getOracleModuleByName(defaultModule);
	}

	protected void initialConnection() {

		if (reposManager == null) {
			reposManager = RepositoryManager.getInstance();
		}
		logger.debug("Get OWB repository instance");

		if (connection == null) {
			try {
				connection = reposManager.openConnection(
						properties.getProperty(OWBPropConstants.USER), // username
						properties.getProperty(OWBPropConstants.PASSWORD), // password
						properties.getProperty(OWBPropConstants.HOST) + ":"
								+ properties.getProperty(OWBPropConstants.PORT) + ":"
								+ properties.getProperty(OWBPropConstants.ORACLE_SID), // connect
																						// string
						RepositoryManager.MULTIPLE_USER_MODE);
			} catch (ConnectionFailureException e) {
				logger.error("Failed to get OWB connection.Program terminates.");
				logger.error(e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(1);
			}
		}
		logger.debug("Get OWB connection on " + properties.getProperty(OWBPropConstants.USER) + "@"
				+ properties.getProperty(OWBPropConstants.HOST) + ":" + properties.getProperty(OWBPropConstants.PORT)
				+ ":" + properties.getProperty(OWBPropConstants.ORACLE_SID));

	}

	public void gracefulExit() {
		gracefulExit(false);
	}

	public void gracefulExit(boolean commitBeforeExit) {

		if (commitBeforeExit) {
			connection.commit();
			logger.info("Commit in OWB");
		} else {
			connection.rollback();
			logger.info("Rollback in OWB");
		}
		if (reposManager != null && reposManager.isConnected()) {
			reposManager.disconnect();
			logger.info("Disconnect from OWB");
		}

		logger.info("Graceful exit");

		// System.exit(0);
	}

}
