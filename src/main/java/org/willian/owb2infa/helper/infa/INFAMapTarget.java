package org.willian.owb2infa.helper.infa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.powercenter.sdk.mapfwk.core.ExpTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.PortType;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.SourceTarget;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformField;
import com.informatica.powercenter.sdk.mapfwk.core.TransformGroup;
import com.informatica.powercenter.sdk.mapfwk.core.TransformHelper;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.repository.RepoPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataConnection;
import org.willian.owb2infa.model.MetadataField;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;

public class INFAMapTarget extends HelperBase {

	static Logger logger = LogManager.getLogger(INFAMapTarget.class);

	public void constructTargetInMap(MetadataOperator op, Folder folder,
			Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String opName = op.getName();

		printSplitLine(logger);
		logger.debug("Construct target " + opName + " in the mapping");
		printSplitLine(logger);

		String opBoundName = op.getBoundName();

		if (opBoundName == null) {
			opBoundName = op.getName();
		}

		String folderName = folder.getName();
		logger.debug("Check and add shortcut if the target is existed in folder "
				+ folderName);

		String checkName = INFAPropConstants.getINFATransPrefix("TARGET")
				+ opBoundName;

		ShortCut targetShortCut = INFAUtils.getShortCutFromFolder(folder,
				checkName, RepositoryObjectConstants.OBJTYPE_TARGET);
		if (targetShortCut != null) {

			logger.debug("Retrieved target shortcut "
					+ targetShortCut.getName());
			addIfNotExistedTarget(op, targetShortCut, folder, mapping,
					owbMapping, expInputSetHM);

		} else {
			logger.debug("Can't find the shortcut for target " + opName
					+ " in folder " + folderName + ". Check or add the target "
					+ opName + " in local folder");
			addIfNotExistedTarget(op, null, folder, mapping, owbMapping,
					expInputSetHM);
		}
	}

	private void addIfNotExistedTarget(MetadataOperator targetOP,
			ShortCut targetSC, Folder folder, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		boolean found = false;
		String tgtName = targetOP.getName();
		String tgtBoundName = targetOP.getBoundName();
		if (tgtBoundName == null) {
			tgtBoundName = targetOP.getName();
		}
		if (targetSC == null) {
			logger.debug("Shortcut for target " + tgtName
					+ " is not created in local folder");
			Target tgt = folder.getTarget(tgtBoundName);
			if (tgt != null) {
				found = checkTargetInMap(targetOP, mapping);

				if (!found) {
					logger.debug("TARGET " + tgtName
							+ " is NOT existed in mapping");
					addTarget(targetOP, null, folder, mapping, owbMapping,
							expInputSetHM);
				}
			} else {
				logger.error("Can't retrieve " + tgtName + " in folder "
						+ folder.getName());
			}
		} else {
			logger.debug("Shortcut for target "
					+ tgtName
					+ " is existed in local folder. Add the shortcut as target in Informatica");
			addTarget(targetOP, targetSC, folder, mapping, owbMapping,
					expInputSetHM);
		}

	}

	private void addTarget(MetadataOperator targetOP, ShortCut targetSC,
			Folder folder, Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		/*
		 * Loading type : CHECK_INSERT, DELETE, DELETE_INSERT, DERIVE_FROM_LCR,
		 * INSERT, INSERT_UPDATE, NONE, TRUNCATE_INSERT, UPDATE, UPDATE_INSERT
		 */
		String loadingType = targetOP.getLoadingType();

		logger.debug("Dealing with target " + tgtName
				+ " with loading type as " + loadingType);

		if (loadingType.equals("DELETE") || loadingType.equals("DELETE_INSERT")
				|| loadingType.equals("DERIVE_FROM_LCR")
				|| loadingType.equals("NONE")
				|| loadingType.equals("TRUNCATE_INSERT")) {

			logger.error("The LOADING_TYPE "
					+ loadingType
					+ " is not supported yet. Change it to default INSERT type.");

			loadingType = "INSERT";
		}
		boolean found = checkTargetInMap(targetOP, mapping);

		if (!found) {
			logger.debug("TARGET "
					+ tgtName
					+ " hasn't been processed in mapping. Ready to add it into mapping");
			// constructSQLTransForTarget(targetOP, mapping, owbMapping,
			// expInputSetHM);
			// connectSQLToTarget(targetOP, targetSC, folder, mapping,
			// expInputSetHM);

			/*
			 * It first judges the loading type of OWB target table. No matter
			 * insert or upsert, a dummy expression with target fields excluding
			 * sequence id will be constructed
			 */
			// String dummyExpr1 =
			// INFAPropConstants.getINFATransPrefix("EXPRESSION") + tgtName;
			// createDummyExprForTarget(dummyExpr1, false, targetOP, mapping,
			// owbMapping, expInputSetHM);

			if (loadingType.equals("CHECK_INSERT")
					|| loadingType.equals("INSERT")) {
				/*
				 * If insert only, add one more dummy expression with sequence
				 * id and link to target
				 */

				String dummyExpr1 = INFAPropConstants
						.getINFATransPrefix("EXPRESSION") + tgtName;
				createDummyExprForTarget(dummyExpr1, true, targetOP, mapping,
						owbMapping, expInputSetHM);

				dealWithInsertTarget(targetOP, targetSC, folder, mapping,
						owbMapping, expInputSetHM);
			}

			if (loadingType.equals("INSERT_UPDATE")
					|| loadingType.equals("UPDATE_INSERT")
					|| loadingType.equals("UPDATE")) {

				String dummyExpr1 = INFAPropConstants
						.getINFATransPrefix("EXPRESSION") + tgtName;
				createDummyExprForTarget(dummyExpr1, false, targetOP, mapping,
						owbMapping, expInputSetHM);

				/*
				 * If update is required, a primary key lookup transform is
				 * added
				 */
				// String invokeCmd = createULKPforTarget(targetOP, folder,
				// mapping, owbMapping, expInputSetHM);

				createLKPforTarget(targetOP, folder, mapping, owbMapping,
						expInputSetHM);

				if (loadingType.equals("UPDATE")) {
					/*
					 * If update only, add dummy expr and lookup to another
					 * dummy expression with sequence id and link to target
					 */
					// dealWithUpdateTarget(targetOP, targetSC, folder, mapping,
					// owbMapping, expInputSetHM, invokeCmd);

					dealWithUpdateTarget(targetOP, targetSC, folder, mapping,
							owbMapping, expInputSetHM);

				} else {

					String dummyExpr2 = INFAPropConstants
							.getINFATransPrefix("EXPRESSION")
							+ tgtName
							+ "_Seq";

					// Boolean createdDummyExpr2 =
					// createDummyExpr2ForTarget(dummyExpr1, dummyExpr2,
					// targetOP, mapping,
					// owbMapping, expInputSetHM, invokeCmd, true);
					Boolean createdDummyExpr2 = createDummyExpr2WithLKPForTarget(
							dummyExpr1, dummyExpr2, targetOP, mapping,
							owbMapping, expInputSetHM, true);
					if (createdDummyExpr2) {
						/*
						 * If update/insert is required, a router and two
						 * update/insert trans are added
						 */
						createRouterforTarget(targetOP, folder, mapping,
								expInputSetHM);

						dealWithUpsertTarget(targetOP, targetSC, folder,
								mapping, owbMapping, expInputSetHM);
					} else {
						logger.error("Failed to create dummy expression 2. Terminate the following tasks");
					}
				}
			}

			logger.debug("Add connnected TARGET " + tgtName + " to mapping");
		}

	}

	private void dealWithUpsertTarget(MetadataOperator targetOP,
			ShortCut targetSC, Folder folder, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		printSplitLine(logger);
		logger.debug("Deal with the update/insert target");
		printSplitLine(logger);

		String repoName = getRepoName();
		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		String tgtBoundName = targetOP.getBoundName();

		if (tgtBoundName == null) {
			tgtBoundName = targetOP.getName();
		}

		MetadataGroup metadataGroup = targetOP.getGroups().get(0);

		Target tgt0 = null;
		Target tgt2 = null;
		ShortCut scIns = null;
		ShortCut scUpd = null;

		logger.debug("Creating Shortcuts to Target");

		if (targetSC == null) {

			tgt0 = folder.getTarget(tgtBoundName);

			tgt2 = (Target) tgt0.clone();
			tgt2.setInstanceName("Upd_" + tgtName);
			tgt0.setInstanceName("Ins_" + tgtName);

			scIns = new ShortCut("Ins_" + tgtName, "shortcut to Target "
					+ tgtName + " for insert", repoName, folder.getName(),
					tgtName, RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);
			scUpd = new ShortCut("Upd_" + tgtName, "shortcut to Target "
					+ tgtName + " for update", repoName, folder.getName(),
					tgtName, RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);

			scIns.setRefObject(tgt0);
			scUpd.setRefObject(tgt2);

			// folder.addTarget(tgt2);
			// folder.addTargetInternal(tgt2);

			logger.debug("Add source and shortcuts to folder");

		} else {
			tgt0 = (Target) targetSC.getRefObject();
			tgt2 = (Target) tgt0.clone();

			scIns = new ShortCut("Ins_" + tgtName, "shortcut to Target "
					+ tgtName + " for insert", repoName, "SHARED", tgtName,
					RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);
			scUpd = new ShortCut("Upd_" + tgtName, "shortcut to Target "
					+ tgtName + " for update", repoName, "SHARED", tgtName,
					RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);

			scIns.setRefObject(tgt0);
			scUpd.setRefObject(tgt2);

			tgt0.setInstanceName(scIns.getName());
			tgt2.setInstanceName(scUpd.getName());

			logger.debug("Add source shortcuts to mapping");
		}

		// if the target table is shortcut from shared folder, then there
		// won't be duplicate targets.
		// Otherwise there will be duplicated shortcuts.
		mapping.addShortCut(scIns);
		mapping.addShortCut(scUpd);

		RowSet insRS = INFAUtils.getExpRowSet(expInputSetHM, "RTR_Ins");
		RowSet updRS = INFAUtils.getExpRowSet(expInputSetHM, "RTR_Upd");

		logger.debug("Validate the output Rowset for Router update group outgoing");
		List<Field> updFieldTmp = updRS.getFields();
		if (updFieldTmp != null) {
			INFAValidator.validateFieldList(updFieldTmp);
		}

		TransformHelper helper = new TransformHelper(mapping);

		OutputSet outputSetUpdTrans1 = helper.updateStrategy(insRS,
				"DD_INSERT", "UPD_Ins_" + tgtName);
		logger.debug("Create the output set for Insert strategy transform");

		OutputSet outputSetUpdTrans2 = helper.updateStrategy(updRS,
				"DD_UPDATE", "UPD_Upd_" + tgtName);
		logger.debug("Create the output set for Update strategy transform");

		RowSet updInsRS = (RowSet) outputSetUpdTrans1.getRowSets().get(0);
		RowSet updUpdRS = (RowSet) outputSetUpdTrans2.getRowSets().get(0);

		logger.debug("Successfully get Update and Insert export rowset");
		java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

		logger.debug("Dealing with the insert ");

		for (MetadataField mField : metadataGroup.getFields()) {
			String mFieldName = mField.getName();
			String outFieldName = mFieldName + "1";
			Field insertField = updInsRS.getField(outFieldName);
			Field tgtField = tgt0.getField(mFieldName);

			logger.debug("Ready to process the target field (" + mFieldName
					+ ")");

			if (insertField != null && tgtField != null) {
				link.put(insertField, tgtField);

				logger.debug("Link Insert strategy transform field to target field : "
						+ insertField.getName() + " --> " + tgtField.getName());
			} else {
				if (insertField == null) {
					logger.debug("Link Insert strategy transform field "
							+ outFieldName + " can't be found ");
				} else {
					logger.debug("Target field " + mFieldName
							+ " can't be found ");
				}
			}
		}
		PortLinkContext linkContext = PortLinkContextFactory
				.getPortLinkContextByMap(link);

		InputSet asqIS = new InputSet(
				updInsRS,
				PortPropagationContextFactory
						.getContextForExcludeColsFromAll(new String[] { "UPSERT_FLAG" }),
				linkContext);

		mapping.writeTarget(asqIS, (Target) scIns.getRefObject());

		mapping.removeTarget(tgt0);

		// logger.debug("Validate the inputset for insert strategy trans");
		// INFAValidator.validateInputSet(asqIS);

		logger.debug("Dealing with the update");

		ArrayList<MetadataField> updFields = metadataGroup.getToUpdateFields();
		ArrayList<Field> includeFields = new ArrayList<Field>();

		link.clear();

		for (MetadataField mField : updFields) {

			String fieldName = mField.getName();
			String outFieldName = fieldName + "2";

			logger.debug("Find update field : " + fieldName);

			// There are field renaming. So we need to add suffix
			Field field = updUpdRS.getField(outFieldName);

			if (field != null) {
				logger.debug("...Get the field object : " + field.getName());

				includeFields.add(field);

				Field tgtField = tgt2.getField(fieldName);
				if (tgtField != null) {
					link.put(field, tgtField);

					logger.debug("Link update strategy transform field "
							+ outFieldName + " to target field " + fieldName);
				} else {
					logger.debug("Target field " + fieldName + " can be found ");
				}

			} else {
				logger.debug(" The field object " + fieldName + " is null");
			}
		}

		logger.debug("Connect primary key ");

		MetadataField mpkField = targetOP.getGroups().get(0).getPrimaryKey();
		String pkFieldName = mpkField.getName();
		String pkFieldInRtr = pkFieldName + "2";

		Field pkField = updUpdRS.getField(pkFieldInRtr);
		Field tgtField = tgt2.getField(pkFieldName);

		includeFields.add(pkField);
		link.put(pkField, tgtField);

		logger.debug("There are " + includeFields.size()
				+ " outgoing update fields");

		logger.debug("The update fields are : " + includeFields.toString());

		PortPropagationContext updRSContext = PortPropagationContextFactory
				.getContextForIncludeCols(includeFields);
		linkContext = PortLinkContextFactory.getPortLinkContextByMap(link);

		InputSet updSet = new InputSet(updUpdRS, updRSContext, linkContext);

		mapping.writeTarget(updSet, (Target) scUpd.getRefObject());
		mapping.removeTarget(tgt2);

		folder.addMapping(mapping);
		folder.addMappingInternal(mapping);

		List<ShortCut> shortcuts = folder.getShortCuts();

		for (ShortCut sc : shortcuts) {

			logger.debug("Find shortcut in local folder : " + sc.getName()
					+ " (folder name : " + sc.getFolderName());
		}

		folder.removeShortCut(scIns);
		folder.removeShortCut(scUpd);

		shortcuts = folder.getShortCuts();

		for (ShortCut sc : shortcuts) {

			logger.debug("After remove some shortcuts, find shortcut in local folder : "
					+ sc.getName() + " (folder name : " + sc.getFolderName());
		}
		// logger.debug("Validate the inputset for update strategy trans");
		// INFAValidator.validateInputSet(updSet);

	}

	private void dealWithUpdateTarget(MetadataOperator targetOP,
			ShortCut targetSC, Folder folder, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		printSplitLine(logger);
		logger.debug("Deal with the update target");
		printSplitLine(logger);

		String repoName = getRepoName();
		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		String tgtBoundName = targetOP.getBoundName();

		if (tgtBoundName == null) {
			tgtBoundName = targetOP.getName();
		}

		String dummyExpr1 = INFAPropConstants.getINFATransPrefix("EXPRESSION")
				+ tgtName;
		String dummyExpr2 = INFAPropConstants.getINFATransPrefix("EXPRESSION")
				+ tgtName + "_Seq";

		Boolean createdDummyExpr2 = createDummyExpr2WithLKPForTarget(
				dummyExpr1, dummyExpr2, targetOP, mapping, owbMapping,
				expInputSetHM, false);

		if (createdDummyExpr2) {
			logger.debug("Successfully created the dummy expression 2");

			RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, dummyExpr2);

			Target tgt = null;

			if (targetSC == null) {
				tgt = folder.getTarget(tgtBoundName);
			} else {
				// mapping.addShortCut(targetSC);
				tgt = (Target) targetSC.getRefObject();
			}

			ShortCut scUpd = null;

			logger.debug("Creating Shortcuts to Target");

			if (targetSC == null) {

				Target tgtUpd = (Target) tgt.clone();
				tgtUpd.setInstanceName("Upd_" + tgtName);

				scUpd = new ShortCut("Upd_" + tgtName, "shortcut to Target "
						+ tgtName + " for update", repoName, folder.getName(),
						tgtName, RepositoryObjectConstants.OBJTYPE_TARGET,
						TransformationConstants.STR_TARGET, ShortCut.LOCAL);

				scUpd.setRefObject(tgtUpd);

				// folder.addTarget(tgt2);
				// folder.addTargetInternal(tgt2);

				logger.debug("Add source and shortcuts to folder");

			} else {
				Target tgtUpd = (Target) tgt.clone();
				tgtUpd.setInstanceName("Upd_" + tgtName);

				scUpd = new ShortCut("Upd_" + tgtName, "shortcut to Target "
						+ tgtName + " for update", repoName, "SHARED", tgtName,
						RepositoryObjectConstants.OBJTYPE_TARGET,
						TransformationConstants.STR_TARGET, ShortCut.LOCAL);

				scUpd.setRefObject(tgtUpd);

				logger.debug("Add source shortcuts to mapping");
			}

			mapping.addShortCut(scUpd);

			INFAValidator.auditTargetConnectionInfo(tgt);

			String transName = INFAPropConstants.getINFATransPrefix("UPSERT")
					+ "Upd_" + tgtName;

			logger.debug("Set the update/insert transform name as " + transName);

			MetadataGroup metadataGroup = targetOP.getGroups().get(0);

			String pkFieldName = metadataGroup.getPrimaryKey().getName();

			TransformHelper helper = new TransformHelper(mapping);

			String strategyExpr = "IIF(UPSERT_FLAG=2 , DD_UPDATE, DD_REJECT )";
			RowSet updRS = (RowSet) helper
					.updateStrategy(expRS, strategyExpr, transName)
					.getRowSets().get(0);

			logger.debug("Add the update/insert transform into mapping with the strategy string as : "
					+ strategyExpr);

			ArrayList<MetadataField> updFields = metadataGroup
					.getToUpdateFields();
			ArrayList<Field> includeFields = new ArrayList<Field>();
			java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

			for (MetadataField mField : updFields) {

				String fieldName = mField.getName();
				Field field = updRS.getField(fieldName);

				includeFields.add(field);

				Field tgtField = tgt.getField(fieldName);
				if (tgtField != null) {
					link.put(field, tgtField);

					logger.debug("Link update strategy transform field "
							+ fieldName + " to target field " + fieldName);
				} else {
					logger.debug("Target field " + fieldName + " can be found ");
				}

			}

			logger.debug("Connect primary key ");

			String pkFieldInRtr = pkFieldName;

			logger.trace("Try to get primary key as " + pkFieldInRtr);

			Field pkField = updRS.getField(pkFieldInRtr);

			if (pkField == null) {

				pkFieldInRtr = "IN_" + pkFieldName;

				logger.trace("...Try to get primary key as " + pkFieldInRtr);

				pkField = updRS.getField(pkFieldInRtr);

			}

			Field tgtField = tgt.getField(pkFieldName);

			includeFields.add(pkField);
			link.put(pkField, tgtField);

			PortPropagationContext updRSContext = PortPropagationContextFactory
					.getContextForIncludeCols(includeFields);
			PortLinkContext linkContext = PortLinkContextFactory
					.getPortLinkContextByMap(link);

			InputSet updSet = new InputSet(updRS, updRSContext, linkContext);

			mapping.writeTarget(updSet, (Target) scUpd.getRefObject());
		}
	}

	private void dealWithInsertTarget(MetadataOperator targetOP,
			ShortCut targetSC, Folder folder, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		printSplitLine(logger);
		logger.debug("Deal with the insert target");
		printSplitLine(logger);

		String repoName = getRepoName();
		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		String tgtBoundName = targetOP.getBoundName();

		if (tgtBoundName == null) {
			tgtBoundName = targetOP.getName();
		}

		String dummyExpr1 = INFAPropConstants.getINFATransPrefix("EXPRESSION")
				+ tgtName;

		String startRS = dummyExpr1;

		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, startRS);

		Target tgt = null;

		if (targetSC == null) {
			tgt = folder.getTarget(tgtBoundName);
		} else {
			// mapping.addShortCut(targetSC);
			tgt = (Target) targetSC.getRefObject();
		}

		ShortCut scIns = null;

		logger.debug("Creating Shortcuts to Target");

		Target tgtIns = (Target) tgt.clone();

		if (targetSC == null) {

			tgtIns.setInstanceName("Ins_" + tgtName);

			scIns = new ShortCut("Ins_" + tgtName, "shortcut to Target "
					+ tgtName + " for insert", repoName, folder.getName(),
					tgtName, RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);

			scIns.setRefObject(tgtIns);

			logger.debug("Add source and shortcuts " + scIns.getName()
					+ " to folder");

		} else {

			scIns = new ShortCut("Ins_" + tgtName, "shortcut to Target "
					+ tgtName + " for insert", repoName, "SHARED", tgtName,
					RepositoryObjectConstants.OBJTYPE_TARGET,
					TransformationConstants.STR_TARGET, ShortCut.LOCAL);

			scIns.setRefObject(tgtIns);

			tgtIns.setInstanceName(scIns.getName());

			logger.debug("Add source shortcuts  " + scIns.getName()
					+ " to mapping");
		}

		mapping.addShortCut(scIns);

		mapping.writeTarget(expRS, (Target) scIns.getRefObject());

		logger.debug("Connect expression transform with target "
				+ scIns.getName());
	}

	/*
	 * The dummy expression transformation has contained all the target field as
	 * well as the sequence and expression
	 */

	private void createDummyExprForTarget(String transName, boolean addSeq,
			MetadataOperator targetOP, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to construct the dummy expression transform for target");
		HelperBase.printSplitLine(logger);

		List<TransformField> transformFields = new ArrayList<TransformField>();
		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

		// Get input fields

		HashSet<String> incomingRelatedOPs = targetOP
				.getIncomingRelationships();
		Iterator<String> inIdentifierIter = incomingRelatedOPs.iterator();

		MetadataGroup inoutGroup = targetOP.getGroups().get(0);

		List<MetadataField> allTargetFields = inoutGroup.getFields();

		// ArrayList<TransformField> outFieldsForDummyExpr = new
		// ArrayList<TransformField>();

		HashSet<String> processedFields = new HashSet<String>();

		Hashtable<String, INFAInputSetUnit> inputsTemp = new Hashtable<String, INFAInputSetUnit>();

		while (inIdentifierIter.hasNext()) {

			String inIdentifier = inIdentifierIter.next();

			logger.debug("Start to contruct the input set of dummy expression 1 for incoming identifier "
					+ inIdentifier);

			RowSet expRS = null;

			for (MetadataField metadataField : allTargetFields) {
				String targetFieldName = metadataField.getName();

				// logger.debug("Processing the target field " + targetFieldName
				// + " into this expression transform");
				ArrayList<MetadataConnection> inputConnections = metadataField
						.getInputConnections();

				if (inputConnections != null && inputConnections.size() > 0) {
					MetadataConnection inConnection = inputConnections.get(0);

					String inField = inConnection.getSourceField();
					String inGroup = inConnection.getSourceGroup();
					String inOperator = inConnection.getSourceOperator();

					// logger.debug("The target field is connecting to the incoming "
					// + inOperator + "." + inGroup + "."
					// + inField);

					MetadataOperator mOP = owbMapping
							.getOperatorByName(inOperator);
					MetadataGroup mGp = mOP.getGroupByName(inGroup);
					MetadataField mFd = mGp.getFieldByName(inField);

					if (!processedFields.contains(targetFieldName)
							&& inIdentifier.equals(inOperator + "." + inGroup)) {

						String expression = mFd.getExpression();
						expression = SQLProcessor.cleanExpression(expression);
						String opType = mOP.getType();

						logger.debug("The field "
								+ targetFieldName
								+ " matches the incoming connection from identifier : "
								+ inIdentifier);
						Field targetField = INFAUtils.convertToField(
								metadataField, FieldType.TRANSFORM);

						if (INFAUtils.isSupportedOWBTransform(opType)) {

							logger.debug("The incomming connection is from supported operator (type : "
									+ opType + ")");
							if (opType.equals("TRANSFORMATION")
									&& mOP.getIncomingRelationships().size() == 0) {

								logger.debug("The incomming connection is from standalone TRANSFORM operator field");

								TransformField tField = new TransformField(
										targetField, PortType.OUTPUT);

								logger.debug("Add "
										+ inField
										+ " of "
										+ inGroup
										+ " of "
										+ inOperator
										+ " into standalone transformField list");

								tField.setExpr(expression);

								transformFields.add(tField);

								processedFields.add(targetFieldName);
							} else {
								if (inputsTemp.get(inIdentifier) == null) {

									logger.debug("Initial InputSet unit instance for in indentifier "
											+ inIdentifier);

									INFAInputSetUnit tempUnit = new INFAInputSetUnit();

									// Initial the rowset
									if (mOP.isSource()) {
										String srcIdentifier = INFAPropConstants
												.getINFATransPrefix("SOURCE")
												+ inOperator;

										expRS = INFAUtils.getExpRowSet(
												owbMapping, expInputSetHM,
												srcIdentifier);

										logger.debug("Get export row set for source "
												+ inOperator);
									} else {
										expRS = INFAUtils.getExpRowSet(
												owbMapping, expInputSetHM,
												inIdentifier);

										logger.debug("Get export row set for identifier "
												+ inIdentifier);

									}

									tempUnit.setRs(expRS);

									// the inIndentifier is different from the
									// identifier used to store in hashtable
									inputsTemp.put(inIdentifier, tempUnit);

									logger.debug("Create inputsTemp for in identifier "
											+ inIdentifier);

								}

								if (inputsTemp.get(inIdentifier).getRs() != null) {

									Field incomingTransField = INFAUtils
											.convertToField(metadataField,
													FieldType.TRANSFORM);

									// TransformField tField1 = new
									// TransformField(targetField,
									// PortType.INPUT_OUTPUT);

									// transformFields.add(tField1);
									// logger.debug("Add " + inField + " of " +
									// inGroup + " of " + inOperator
									// + " into transformField list");

									Field sourcingField = inputsTemp
											.get(inIdentifier).getRs()
											.getField(inField);

									if (sourcingField != null) {

										inputsTemp.get(inIdentifier)
												.getIncludeFields()
												.add(sourcingField);

										incomingTransField.setName("IN_"
												+ targetField.getName());

										inputsTemp
												.get(inIdentifier)
												.getExprLink()
												.put(sourcingField,
														incomingTransField);

										TransformField outTransTField = new TransformField(
												targetField, PortType.OUTPUT);

										String dataType = metadataField
												.getData_type();

										if (dataType.equals("VARCHAR2")
												|| dataType.equals("CHAR")
												|| dataType.equals("VARCHAR")) {
											outTransTField
													.setExpr("LTRIM(RTRIM("
															+ incomingTransField
																	.getName()
															+ "))");
										} else {
											outTransTField
													.setExpr(incomingTransField
															.getName());
										}
										logger.debug("Target Field "
												+ incomingTransField.getName()
												+ " is connected from "
												+ sourcingField.getName()
												+ " of " + inGroup + " of "
												+ inOperator);

										transformFields.add(outTransTField);
										processedFields.add(targetFieldName);
									} else {
										logger.debug("Can't find the sourcing field "
												+ inField
												+ " in the export RowSet for indentifier "
												+ inIdentifier);
									}
								}
							}
						} else if (opType.equals("CONSTANT")) {

							logger.debug("The incomming connection is from CONSTANT field");

							TransformField tField = new TransformField(
									targetField, PortType.OUTPUT);

							logger.debug("Add " + inField + " of " + inGroup
									+ " of " + inOperator
									+ " into transformField list");
							if (expression.contains(".")) {
								logger.error("The expression for target field "
										+ targetFieldName
										+ " may contain database related expression.");
								tField.setExpr("-- The expression for target field "
										+ targetFieldName
										+ " may contain database related expression. Please create SQL transform for this field. The expression is : "
										+ expression);
							} else {
								tField.setExpr(expression);
							}

							transformFields.add(tField);

							processedFields.add(targetFieldName);

						} else if (opType.equals("SEQUENCE")) {

							if (addSeq) {

								String exprString = "double(15,0) "
										+ targetFieldName
										+ "=:SP.SEQ_NEXTVAL('" + expression
										+ "')";

								TransformField seqField = new TransformField(
										exprString);

								transformFields.add(seqField);

								processedFields.add(targetFieldName);

							} else {
								// bypass
								logger.debug("The incoming connection is from SEQUENCE. The sequence id will be skipped here.");
							}
						} else {

							logger.error("The operaor "
									+ inOperator
									+ " ("
									+ opType
									+ ") is not supported.Failed to connect to target field "
									+ inField);

							TransformField tField = new TransformField(
									targetField, PortType.OUTPUT);

							tField.setExpr("-- This field is from unsupported OWB operator. Origin expression is : "
									+ expression);
							transformFields.add(tField);
							processedFields.add(targetFieldName);

							logger.debug("For compatible,  only create dummy target filed");

						}

					} else {

						// logger.debug(inField + " of " + inGroup + " of " +
						// inOperator
						// +
						// " has been added into transformField list or not match with the identifier. Skip it.");
					}
				} else {

					// logger.debug("Bypass the field " + targetFieldName
					// + " becasue it doesn't match the incoming identifier (" +
					// inIdentifier
					// + ") we are looking for ");
				}

			}

		}

		for (String iden : inputsTemp.keySet()) {

			if (inputsTemp.get(iden).getRs() != null) {
				logger.debug("Ready to construct inputset for in identifier "
						+ iden + " and add into allInputSets");

				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(inputsTemp.get(iden)
								.getIncludeFields());
				PortLinkContext linkContext = PortLinkContextFactory
						.getPortLinkContextByMap(inputsTemp.get(iden)
								.getExprLink());

				InputSet inSet = new InputSet(inputsTemp.get(iden).getRs(),
						linkPropContext, linkContext);

				allInputSets.add(inSet);

				logger.debug("Add " + iden + " into allInputSets");
			}
		}

		logger.debug("Finish the target field process exclude sequence id (if any)");

		ExpTransformation dummyExpr = new ExpTransformation(transName,
				transName, "Dummy Expression Transformation for target",
				transName, mapping, allInputSets, transformFields, null);

		logger.debug("Add Dummy Expression transform  " + transName
				+ " into the mapping");

		RowSet exprRS = dummyExpr.apply().getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, transName, exprRS);
		logger.debug("Add the export rowset of Dummy Expression transform  "
				+ transName + " into the hashmap memory");

		// INFAValidator.validateTransformInstance(dummyExpr);

	}

	private boolean createDummyExpr2WithLKPForTarget(String dummyExpr1,
			String dummyExpr2, MetadataOperator targetOP, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM,
			boolean needInsert) throws InvalidTransformationException {

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to construct dummy expression 2 (with sequence id) with connected lookup for target");
		HelperBase.printSplitLine(logger);

		// boolean foundSeq = false;
		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		List<TransformField> transformFields = new ArrayList<TransformField>();

		MetadataGroup inoutGroup = targetOP.getGroups().get(0);

		List<MetadataField> allTargetFields = inoutGroup.getFields();

		RowSet exp1RS = INFAUtils.getExpRowSet(expInputSetHM, dummyExpr1);

		logger.debug("Get the rowset with " + exp1RS.size()
				+ " fields from dummy expression 1 : " + dummyExpr1);

		String lkpTransName = INFAPropConstants.getINFATransPrefix("LOOKUP")
				+ tgtName;

		RowSet lookupRS = INFAUtils.getExpRowSet(expInputSetHM, lkpTransName);

		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();
		ArrayList<Field> includeFields = new ArrayList<Field>();

		ArrayList<Field> lookupFields = new ArrayList<Field>();

		MetadataField mpkField = targetOP.getGroups().get(0).getPrimaryKey();
		String pkFieldName = mpkField.getName();

		Field pkField = lookupRS.getField(pkFieldName);

		lookupFields.add(pkField);

		Field inPKField = (Field) pkField.clone();
		String inputPKName = "IN_" + pkFieldName;
		inPKField.setName(inputPKName);

		PortPropagationContext linkPropContext = PortPropagationContextFactory
				.getContextForIncludeCols(lookupFields);

		java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

		link.put(pkField, inPKField);

		PortLinkContext linkContext = PortLinkContextFactory
				.getPortLinkContextByMap(link);

		InputSet lookupInset = new InputSet(lookupRS, linkPropContext,
				linkContext);

		allInputSets.add(lookupInset);

		logger.debug("Add lookup connection");

		// 1 for insert, 2 for update
		String exprString2 = "double(1,0) UPSERT_FLAG= IIF(ISNULL("
				+ inputPKName + ")," + "1,2)";
		TransformField flagField = new TransformField(exprString2);
		transformFields.add(flagField);
		logger.debug("Add UPSERT_FLAG column");

		for (MetadataField metadataField : allTargetFields) {
			String targetFieldName = metadataField.getName();
			ArrayList<MetadataConnection> inputConnections = metadataField
					.getInputConnections();

			if (inputConnections != null && inputConnections.size() > 0) {
				MetadataConnection inConnection = inputConnections.get(0);

				String inField = inConnection.getSourceField();
				String inGroup = inConnection.getSourceGroup();
				String inOperator = inConnection.getSourceOperator();

				MetadataOperator mOP = owbMapping.getOperatorByName(inOperator);
				MetadataGroup mGp = mOP.getGroupByName(inGroup);
				MetadataField mFd = mGp.getFieldByName(inField);

				String opType = mOP.getType();

				if (opType.equals("SEQUENCE")) {

					String expression = mFd.getExpression();

					int dotPosition = expression.indexOf(".");
					expression = expression.substring(0, dotPosition);
					expression = expression.replace("\"", "");

					String exprString = null;
					if (lookupRS == null) {
						exprString = "double(15,0) " + targetFieldName
								+ "=:SP.SEQ_NEXTVAL('" + expression + "')";

						logger.debug("The primary key lookup is null. So the expression for sequence id is : "
								+ exprString);

					} else {

						if (needInsert) {
							exprString = "double(15,0) " + targetFieldName
									+ "= IIF(ISNULL(" + inputPKName + "),"
									+ ":SP.SEQ_NEXTVAL('" + expression + "'),"
									+ inputPKName + ")";

							logger.debug("Input lookup cmd is not null and require INSERT operation. So the expression for sequence id is : "
									+ exprString);

							// 1 for insert, 2 for update
							// String exprString2 =
							// "double(1,0) UPSERT_FLAG= IIF(ISNULL(" +
							// inputPKName + ")," + "1,2)";
							// TransformField flagField = new
							// TransformField(exprString2);
							//
							// transformFields.add(flagField);
						} else {
							exprString = "double(15,0) " + targetFieldName
									+ "= IIF(ISNULL(" + inputPKName + "),"
									+ "0," + inputPKName + ")";

							logger.debug("Input lookup cmd is not null and only need to update target. So the expression for sequence id is : "
									+ exprString);
						}
					}
					TransformField seqField = new TransformField(exprString);

					transformFields.add(seqField);

					logger.debug("Found sequence id and add it to transform field list");
					// foundSeq = true;
				} else {
					if (exp1RS != null) {
						logger.debug("Ready to add " + inField + " of "
								+ inGroup + " of " + inOperator
								+ " into input field " + targetFieldName
								+ " of dummy expression 2");

						Field sourcingField = exp1RS.getField(targetFieldName);

						if (sourcingField != null) {

							includeFields.add(sourcingField);

							logger.debug("Transform Field " + targetFieldName
									+ " is connected from expression 1");
						} else {
							logger.error("Can't find the incoming field "
									+ targetFieldName
									+ " from upward expression 1 rowset");
						}

					} else {
						logger.error("Can't find the export RowSet for dummy expression 1 ");
					}
				}
			}

		}

		if (exp1RS != null) {

			linkPropContext = PortPropagationContextFactory
					.getContextForIncludeCols(includeFields);

			InputSet inSet = new InputSet(exp1RS, linkPropContext);

			allInputSets.add(inSet);

			logger.debug("Add expression 1 as the input set ");
		}

		logger.debug("Before creating the dummy expression2 " + dummyExpr2
				+ ", there are " + transformFields.size()
				+ " transform fields.");

		TransformHelper helper = new TransformHelper(mapping);
		RowSet exp2RS = (RowSet) helper
				.expression(allInputSets, transformFields, dummyExpr2)
				.getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, dummyExpr2, exp2RS);
		logger.debug("Add the export rowset (" + exp2RS.size()
				+ " elements) of Dummy Expression transform  " + dummyExpr2
				+ " into the hashmap memory");

		return true;
	}

	/*
	 * The connected lookup transformation is to identify whether the record is
	 * existed in target and hence determine insert or update
	 */
	private void createLKPforTarget(MetadataOperator targetOP, Folder folder,
			Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		String tgtBoundName = targetOP.getBoundName();

		if (tgtBoundName == null) {
			tgtBoundName = targetOP.getName();
		}
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to construct the connected lookup transform for target "
				+ tgtName);
		HelperBase.printSplitLine(logger);

		String transName = INFAPropConstants.getINFATransPrefix("LOOKUP")
				+ tgtName;

		logger.debug("Get business keys");
		MetadataGroup group = targetOP.getGroups().get(0);

		ArrayList<MetadataField> bizFields = group.getBusinessKeyList();

		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

		StringBuffer condition = new StringBuffer();

		String dummyExpr1 = INFAPropConstants.getINFATransPrefix("EXPRESSION")
				+ tgtName;

		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, dummyExpr1);

		logger.debug("Get rowset of dummy expression 1 ");

		ArrayList<Field> includeFields = new ArrayList<Field>();
		java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

		for (MetadataField mField : bizFields) {

			if (expRS != null) {

				Field iField = expRS.getField(mField.getName());

				logger.debug("Get upward field " + iField.getName());

				// Use the same sourcing field
				Field tField = INFAUtils.convertToField(mField,
						FieldType.TRANSFORM);

				logger.debug("Get target field " + tField.getName());

				String bizKeyName = tField.getName();
				// Rename incoming fields
				tField.setName("IN_" + bizKeyName);

				if (iField != null && tField != null) {
					link.put(iField, tField);
					includeFields.add(iField);

					logger.debug("Link upward field " + iField.getName()
							+ " to target field " + tField.getName());

				}

				String clause = iField.getName() + " = " + tField.getName();

				if (condition.length() > 0) {
					condition.append(" AND " + clause);
				} else {
					condition.append(clause);

				}
				logger.debug("...Add biz key " + bizKeyName + " as "
						+ tField.getName());

			} else {
				logger.error("Can't find the export RowSet for dummy expression 1 ");
			}

		}

		logger.debug("Construct the lookup condition as : "
				+ condition.toString());

		if (expRS != null) {
			PortPropagationContext linkPropContext = PortPropagationContextFactory
					.getContextForIncludeCols(includeFields);
			PortLinkContext linkContext = PortLinkContextFactory
					.getPortLinkContextByMap(link);

			InputSet inSet = new InputSet(expRS, linkPropContext, linkContext);

			allInputSets.add(inSet);

			logger.debug("Construct the allInputSets for lookup transform");

			String checkName = INFAPropConstants.getINFATransPrefix("TARGET")
					+ tgtBoundName;

			ShortCut scTgt = INFAUtils.getShortCutFromFolder(folder, checkName,
					RepositoryObjectConstants.OBJTYPE_TARGET);
			SourceTarget lookupTable = null;
			if (scTgt == null) {

				scTgt = INFAUtils.getShortCutFromFolder(folder, checkName,
						RepositoryObjectConstants.OBJTYPE_SOURCE);

				if (scTgt == null) {
					lookupTable = folder.getTarget(tgtBoundName);

					if (lookupTable != null) {
						logger.debug("Get target from folder");
					}
				} else {

					lookupTable = (Source) scTgt.getRefObject();

					logger.debug("Get source table from shortcut");

				}
			} else {
				lookupTable = (Target) scTgt.getRefObject();

				logger.debug("Get target table from shortcut");
			}

			logger.debug("To get primary key");

			TransformHelper helper = new TransformHelper(mapping);

			OutputSet lkpOutputSet = helper.lookup(allInputSets, lookupTable,
					condition.toString(), transName);

			RowSet lookupRS = (RowSet) lkpOutputSet.getRowSets().get(0);

			INFAUtils.addToExpRowSetHM(expInputSetHM, transName, lookupRS);

			logger.debug("The connected lookup transform has been added into mapping");

		} else {
			logger.error("Can't find the rowset for dummy expression 1 ");
		}
	}

	private void createRouterforTarget(MetadataOperator targetOP,
			Folder folder, Mapping mapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to create Router for insert/update target");
		HelperBase.printSplitLine(logger);

		String tgtName = targetOP.getName();

		tgtName = INFAUtils.toCamelCase(tgtName);

		String dummyExpr2 = INFAPropConstants.getINFATransPrefix("EXPRESSION")
				+ tgtName + "_Seq";

		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, dummyExpr2);

		// create helper
		TransformHelper helper = new TransformHelper(mapping);
		// Create a TransformGroup
		List<TransformGroup> transformGrps = new ArrayList<TransformGroup>();
		TransformGroup transGrp = new TransformGroup("INSERT",
				"UPSERT_FLAG = 1");
		transformGrps.add(transGrp);
		transGrp = new TransformGroup("UPDATE", "UPSERT_FLAG = 2");
		transformGrps.add(transGrp);

		OutputSet outputSetRtr = helper.router(expRS, transformGrps,
				"RTR_Ins_Upd");
		RowSet insRS = outputSetRtr.getRowSets().get(0);
		RowSet updRS = outputSetRtr.getRowSets().get(1);

		INFAUtils.addToExpRowSetHM(expInputSetHM, "RTR_Ins", insRS);
		INFAUtils.addToExpRowSetHM(expInputSetHM, "RTR_Upd", updRS);
		logger.debug("Add the export rowset of Router transform RTR_Ins_Upd into the hashmap memory");

	}

	private boolean checkTargetInMap(MetadataOperator targetOP, Mapping mapping) {

		boolean found = false;

		String inTgtName = targetOP.getName();

		List<Target> targets = mapping.getTargets();
		Iterator<Target> tgtIter = targets.iterator();
		while (tgtIter.hasNext()) {
			Target tgt = tgtIter.next();

			String tgtName = tgt.getName();
			String tgtInstName = tgt.getInstanceName();
			if (tgtName.equals(inTgtName) || tgtInstName.equals(inTgtName)) {
				found = true;
			}

		}

		if (found) {
			logger.debug("Target " + inTgtName + " is existed in mapping");
		}
		return found;
	}

	private String getRepoName() {
		String repoName = null;

		String confName = "pcconfig.properties";
		Properties properties = null;
		try {
			properties = getLocalProps(confName);

			repoName = properties
					.getProperty(RepoPropsConstants.TARGET_REPO_NAME);

		} catch (IOException ioExcp) {
			logger.error(ioExcp.getMessage());
			logger.error("Error reading " + confName + " file.");
			logger.error(confName
					+ " file not found. Add Directory containing this file to ClassPath");
			System.exit(0);
		}

		return repoName;
	}
}
