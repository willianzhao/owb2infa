package org.willian.owb2infa.helper.infa;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.connection.SourceTargetType;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataOperator;

public class INFASource {

	private static Logger logger = LogManager.getLogger(INFASource.class);

	public void createSource(MetadataOperator operator, Folder folder, Repository rep) {
		String opName = operator.getName();

		logger.debug("Working on the " + opName + " as source");

		String opBoundName = operator.getBoundName();

		if (opBoundName == null) {
			opBoundName = opName;
		}

		if (!INFAUtils.isLoadControlTable(opBoundName)) {
			boolean shortCutCreated = false;

			shortCutCreated = createSourceAsShortCut(operator, folder, rep);

			if (!shortCutCreated) {

				logger.debug("Can't create the shortcut for " + opName);

				if (folder.getSource(opBoundName) != null) {
					logger.debug("The source " + opBoundName + " has existed in folder " + folder.getName());
				} else {
					logger.debug("The source " + opBoundName + " has not been created in folder " + folder.getName());
					createSourceInLocalFolder(operator, folder);
				}
			}
		} else {
			logger.debug("Found DW_LOAD_CONTROL_TBL as source in OWB mapping. It has been deprecated in Informatica");
		}
	}

	private boolean createSourceAsShortCut(MetadataOperator operator, Folder folder, Repository rep) {

		boolean actionSuccess = false;
		String name = operator.getName();

		String opBoundName = operator.getBoundName();

		if (opBoundName == null) {
			opBoundName = name;
		}

		String owbLoc = operator.getLocation();
		String infaLoc = INFAPropConstants.getINFALocation("SOURCE", owbLoc);

		Source source = null;

		logger.debug("Ready to create shortcut for source " + name);

		try {
			source = getSourceFromShareFolder(rep, opBoundName, infaLoc);
		} catch (RepoOperationException | MapFwkReaderException e) {
			logger.debug("Can't find source " + opBoundName + " from share folder");
		}
		if (source != null) {
			String sourceName = source.getName();
			logger.debug(sourceName + " is existed in share folder. Ready to create shortcut from this source");
			ShortCut sourceShortCut = createShortCutFromSource(source, rep, folder);

			if (sourceShortCut != null) {
				logger.debug("Create Shortcut " + sourceShortCut.getName() + " for source " + sourceName
						+ " on folder " + sourceShortCut.getFolderName());

			}

			folder.addShortCut(sourceShortCut);
			folder.addShortCutInternal(sourceShortCut); // for fetch
			// functionality
			logger.debug("Add the shortcut " + sourceShortCut.getName() + " for the shared folder source "
					+ opBoundName + " into the folder " + folder.getName());
			actionSuccess = true;
		} else {

			logger.debug("Source " + opBoundName + " can't be found in share folder");
			actionSuccess = false;
		}
		return actionSuccess;
	}

	public static Source getSourceFromShareFolder(Repository rep, final String checkName, final String infaLoc)
			throws RepoOperationException, MapFwkReaderException {

		logger.debug("Get source from shared folder for object "+checkName);

		List<Folder> folders = rep.getFolders(new INameFilter() {

			public boolean accept(String name) {
				logger.debug("check folder " + name);
				return name.equals("SHARED");
			}
		});

		if (folders.size() > 0) {

			Folder temp = (Folder) folders.get(0);
			logger.debug("Find shared folder " + temp.getName());

			Source mySource = getSoureFromFolder(temp, checkName, infaLoc);

			return mySource;
		} else {
			logger.error("Failed to find the shared folder. It's a fatal error. Program exit.");
			System.exit(1);
		}

		return null;
	}

	public static ShortCut createShortCutFromSource(Source src, Repository rep, Folder folder) {

		String srcTblName = src.getName();
		String repoName = rep.getRepoConnectionInfo().getTargetRepoName();
		// String scName = INFAPropConstants.getINFATransPrefix("SHORTCUT") +
		// srcTblName;
		// String scName = srcTblName;
		ShortCut scSrc = new ShortCut(srcTblName, "shortcut to source " + srcTblName, repoName, "SHARED", srcTblName,
				RepositoryObjectConstants.OBJTYPE_SOURCE, TransformationConstants.STR_SOURCE, ShortCut.LOCAL);

		scSrc.setRefObject(src);

		return scSrc;
	}

	private void createSourceInLocalFolder(MetadataOperator operator, Folder folder) {

		String name = operator.getName();
		String boundName = operator.getBoundName();

		if (boundName == null) {
			boundName = name;
		}
		String business_name = operator.getBusiness_name();
		String owbLoc = operator.getLocation();
		String infaLoc = null;
		if (boundName.startsWith("STG") || boundName.endsWith("VW")) {

			owbLoc = "WAREHOUSE_TARGET";
			infaLoc = INFAPropConstants.getINFALocation("SOURCE", owbLoc);
		} else {

			owbLoc = "PRODUCTION_SOURCE";
			infaLoc = INFAPropConstants.getINFALocation("SOURCE", owbLoc);
		}
		logger.debug("Create source " + boundName + " in local folder db (" + infaLoc + ") for operator source " + name);

		List<Field> fields = new ArrayList<Field>();

		ArrayList<MetadataGroup> groups = operator.getGroups();
		if (groups != null) {
			for (MetadataGroup group : groups) {
				INFAUtils.addGroupToFields(group, fields, FieldType.SOURCE);
			}

			ConnectionInfo info = INFAUtils.getRelationalConnectionInfo(SourceTargetType.Oracle, infaLoc);

			Source source = new Source(boundName, business_name, "The source metadata is migrated from OWB operator "
					+ name, name, info);
			logger.debug("Add " + fields.size() + " fields into source " + source.getName());
			source.setFields(fields);
			folder.addSource(source);
			folder.addSourceInternal(source);

			logger.debug("Source " + source.getBusinessName() + " has been added into folder "
					+ folder.getBusinessName());
		} else {
			logger.error("The source has no group");
		}
	}

	public static Source getSoureFromFolder(Folder toCheckFolder, final String checkName, final String infaLoc) {

		logger.debug("Check if source " + checkName + " is existed in folder " + toCheckFolder.getName() + " (dbd: "
				+ infaLoc + ")");

		Source localSource = toCheckFolder.getSource(checkName);

		if (localSource != null) {
			return localSource;
		} else {

			try {
				List<Source> sourceList = null;

				// if (infaLoc != null && !infaLoc.equals("*")) {
				// sourceList =
				// toCheckFolder.fetchSourcesFromRepositoryusingDbdSeparator(new
				// INameFilter() {
				//
				// public boolean accept(String name) {
				// return name.equalsIgnoreCase(checkName);
				// }
				// }, infaLoc);
				// } else {
				sourceList = toCheckFolder.fetchSourcesFromRepository(new INameFilter() {

					public boolean accept(String name) {
						// logger.debug("...Comparing source " + name +
						// "(" +
						// name.length() + ") with " + checkName
						// + " . The result is " + name.matches(".*" +
						// checkName
						// +
						// ".*"));
						// return name.matches(".*" + checkName + ".*");
						return name.equalsIgnoreCase(checkName);
					}
				});
				// }

				// List<Source> sourceList = toCheckFolder.getSources();
				// List<Source> sourceList =
				// toCheckFolder.fetchSourcesFromRepository();
				//
				// if (sourceList != null && sourceList.size() > 0) {
				//

				if (sourceList != null) {
					for (Source source : sourceList) {

						String name = source.getName();
						logger.debug("...Checking source " + name);

						if (name.equalsIgnoreCase(checkName)) {
							logger.debug("Found source " + name);
							return source;
						}
					}
				}
				// }
			} catch (RepoOperationException | MapFwkReaderException e) {
				logger.error(HelperBase.getStackTrace(e));
			}

		}
		return null;
	}
}
