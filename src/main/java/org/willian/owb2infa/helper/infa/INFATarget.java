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
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataOperator;

public class INFATarget {

	private static Logger logger = LogManager.getLogger(INFATarget.class);

	public void createTarget(MetadataOperator operator, Folder folder, Repository rep) {

		boolean shortCutCreated = false;

		shortCutCreated = createTargetAsShortCut(operator, folder, rep);

		if (!shortCutCreated) {
			logger.debug("Can't find the shortcut for " + operator.getName());
			createTargetInLocalFolder(operator, folder);
		}
	}

	private boolean createTargetAsShortCut(MetadataOperator operator, Folder folder, Repository rep) {
		boolean actionSuccess = false;
		Target tgt = null;
		String checkName = operator.getBoundName();

		if (checkName == null) {
			checkName = operator.getName();
		}

		try {
			tgt = getTargetFromShareFolder(rep, checkName);
		} catch (RepoOperationException | MapFwkReaderException e) {
			logger.debug("Can't find the target " + checkName + " in Shared folder");
		}
		if (tgt != null) {
			ShortCut tgtShortCut = createShortCutFromTarget(tgt, rep, folder);

			if (tgtShortCut != null) {
				logger.debug("Create Shortcut " + tgtShortCut.getName() + " for target " + tgt.getName());
			}
			folder.addShortCut(tgtShortCut);
			folder.addShortCutInternal(tgtShortCut);

			logger.debug("Add shortcut " + tgtShortCut.getName() + " for target " + tgt.getName() + " into folder "
					+ folder.getName());
			actionSuccess = true;
		} else {
			actionSuccess = false;
		}
		return actionSuccess;
	}

	public static Target getTargetFromShareFolder(Repository rep, final String checkName)
			throws RepoOperationException, MapFwkReaderException {
		List<Folder> folders = rep.getFolders(new INameFilter() {

			public boolean accept(String name) {
				return name.equals("SHARED");
			}
		});

		Folder temp = folders.get(0);

		Target myTarget = getTargetFromFolder(temp, checkName);
		return myTarget;
	}

	public static ShortCut createShortCutFromTarget(Target tgt, Repository rep, Folder folder) {
		String tgtTblName = tgt.getName();
		String repoName = rep.getRepoConnectionInfo().getTargetRepoName();
		// String scName = INFAPropConstants.getINFATransPrefix("SHORTCUT") +
		// tgtTblName;

		String scName = INFAPropConstants.getINFATransPrefix("TARGET") + tgtTblName;
		ShortCut scTgt = new ShortCut(scName, "shortcut to taget " + tgtTblName, repoName, "SHARED", tgtTblName,
				RepositoryObjectConstants.OBJTYPE_TARGET, TransformationConstants.STR_TARGET, ShortCut.LOCAL);

		scTgt.setRefObject(tgt);

		return scTgt;
	}

	private void createTargetInLocalFolder(MetadataOperator operator, Folder folder) {
		String name = operator.getName();
		String boundName = operator.getBoundName();

		if (boundName == null) {
			boundName = name;
		}

		String business_name = operator.getBusiness_name();
		String owbLoc = operator.getLocation();
		String infaLoc = INFAPropConstants.getINFALocation("TARGET", owbLoc);

		logger.debug("Create Target " + boundName + " in local folder for operator target " + name);

		List<Field> fields = new ArrayList<Field>();
		ArrayList<MetadataGroup> groups = operator.getGroups();

		for (MetadataGroup group : groups) {
			INFAUtils.addGroupToFields(group, fields, FieldType.TARGET);
		}
		ConnectionInfo info = INFAUtils.getRelationalConnectionInfo(SourceTargetType.Oracle, infaLoc);
		Target target = new Target(boundName, business_name, "The target metadata is migrated from OWB operator "
				+ name, boundName, info);
		target.setFields(fields);
		logger.debug("Add " + fields.size() + " fields into target " + target.getName());

		folder.addTarget(target);
		folder.addTargetInternal(target);

		logger.debug("Target " + target.getBusinessName() + " has been added into folder " + folder.getBusinessName());
	}

	public static Target getTargetFromFolder(Folder toCheckFolder, final String checkName) {

		logger.debug("Check if " + checkName + " is existed in folder " + toCheckFolder.getName());
		try {
			List<Target> targetList = toCheckFolder.fetchTargetsFromRepository(new INameFilter() {

				public boolean accept(String name) {
					return name.equalsIgnoreCase(checkName);
				}
			});

			if (targetList != null && targetList.size() > 0) {
				Target tgtFound = targetList.get(0);
				logger.debug("Found " + tgtFound.getName());
				return tgtFound;
			}
		} catch (RepoOperationException | MapFwkReaderException e) {
			logger.debug(HelperBase.getStackTrace(e));
		}
		return null;
	}

}
