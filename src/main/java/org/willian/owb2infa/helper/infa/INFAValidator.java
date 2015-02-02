package org.willian.owb2infa.helper.infa;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.informatica.powercenter.sdk.mapfwk.connection.ConnectionInfo;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.GroupSet;
import com.informatica.powercenter.sdk.mapfwk.core.IOutputField;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.PortDef;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.SQLTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Target;
import com.informatica.powercenter.sdk.mapfwk.core.TransformFieldAttrDefinition;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationProperties;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.metaextension.MetaExtension;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import org.willian.owb2infa.helper.HelperBase;

public class INFAValidator {

	static Logger logger = LogManager.getLogger(INFAValidator.class);

	public static void doValidation(Repository rep, Mapping mapping) {
		try {
			validateRepository(rep);
		} catch (RepoOperationException | MapFwkReaderException e) {
			e.printStackTrace();
		}
		validateTransformFields(mapping);

		validateTargetFields(mapping);
	}

	public static void validateRepository(Repository rep) throws RepoOperationException, MapFwkReaderException {

		HelperBase.printSplitLine(logger);
		logger.debug("Validate repository");
		HelperBase.printSplitLine(logger);

		List<Folder> folders = rep.getModifiedFolders();

		logger.debug("Found " + folders.size() + " folders");

		for (Folder folder1 : folders) {
			logger.debug("Found folder " + folder1.getName());

			List<Mapping> mappings = folder1.getMappings();

			for (Mapping mapping1 : mappings) {
				logger.debug("Found mapping " + mapping1.getName());

				List<Source> sources = mapping1.getSources();

				for (Source source : sources) {
					logger.debug("Found Source " + source.getName());

					List<Field> sourceFields = source.getFields();

					validateFieldList(sourceFields);
				}

				List<Target> targets = mapping1.getTargets();

				for (Target target : targets) {
					logger.debug("Found Target " + target.getName());

					List<Field> tgtFields = target.getFields();

					validateFieldList(tgtFields);
				}

				List<Transformation> trans = mapping1.getTransformations();
				for (Transformation tran : trans) {
					logger.debug("Found transformation " + tran.getName());
				}
			}
		}
	}

	public static void validateFieldList(List<Field> list) {
		HelperBase.printSplitLine(logger);
		logger.debug("...There are " + list.size() + " fields...");

		Iterator<Field> itFields = list.iterator();
		while (itFields.hasNext()) {
			Field field = itFields.next();
			logger.debug(field.getName() + "\tType: " + field.getDataType() + "\tGroup: " + field.getGroupName()
					+ "\tNullable : " + field.isNotNull() + "\tPrecision: " + field.getPrecision() + "\tScale :"
					+ field.getScale());

		}
	}

	public static void validateTargetFields(Mapping mapping) {
		List<Target> targets = mapping.getTargets();
		if (targets != null && targets.size() > 0) {
			Target tgt = targets.get(0);
			List<PortDef> pdList = tgt.getPortDefs();

			HelperBase.printSplitLine(logger);
			logger.debug(("Target fields"));
			HelperBase.printSplitLine(logger);

			for (PortDef pd : pdList) {
				printConnectorInfo(pd);

			}
		}
	}

	public static void validateTransformFields(Mapping mapping) {

		HelperBase.printSplitLine(logger);
		logger.debug("Validate Transform Fields");
		HelperBase.printSplitLine(logger);

		List<Transformation> transformations = mapping.getTransformations();
		Iterator<Transformation> transIter = transformations.iterator();
		while (transIter.hasNext()) {
			Transformation trans = transIter.next();
			// logger.debug(("==================================1"));
			logger.debug("Validate transform " + trans.getName());
			//
			// @SuppressWarnings("unchecked")
			// List<TransformGroup> transGroups = trans.getTransformGroups();
			//
			// logger.debug("...There are " + transGroups.size()
			// + " transform groups");
			// for (TransformGroup tGroup : transGroups) {
			// logger.debug("......Transform Group " + tGroup.getGroupName()
			// + " is " + tGroup.getPortType() + " port type");
			// }
			//
			// List<GroupSet> transGroupSets = trans.getGroupSets();
			//
			// logger.debug("...There are " + transGroupSets.size() +
			// " group set");
			// for (GroupSet tGroup : transGroupSets) {
			// logger.debug("......Group set " + tGroup.getName() + " is "
			// + tGroup.getGroupType() + " type");
			//
			// List<Field> outFields = tGroup.getOutputFields();
			//
			// validateFieldList(outFields);
			//
			// }
			//
			// List<InputSet> ipSet = trans.getTransContext().getInputSets();
			// for (InputSet ip : ipSet) {
			// RowSet iRS = ip.getInRowSet();
			// logger.debug("...Validate tranform context inputset InRowSet");
			// validateFieldList(iRS.getFields());
			// RowSet oRS = ip.getOutRowSet();
			// logger.debug("...Validate tranform context inputset OutRowSet");
			// validateFieldList(oRS.getFields());
			// }
			//			HelperBase.printSplitLine(logger);
			logger.debug(("2"));
			HelperBase.printSplitLine(logger);

			List<InputSet> ipSet1 = trans.getTransContext().getInputSets();
			Iterator<InputSet> iter = ipSet1.iterator();

			logger.debug("Found " + ipSet1.size() + " InputSet");
			while (iter.hasNext()) {
				logger.debug("Validate inputSet");
				InputSet isetElement = iter.next();

				List<IOutputField> outFields = isetElement.getOutputFields();
				List<Field> fields = new ArrayList<Field>();
				for (IOutputField tField : outFields) {

					fields.add(tField.getField());
				}
				validateFieldList(fields);

				List<PortDef> pdList = isetElement.getPortDefs();

				for (PortDef pd : pdList) {
					printConnectorInfo(pd);

				}
			}
			//			HelperBase.printSplitLine(logger);
			//			logger.debug(("3"));
			//			HelperBase.printSplitLine(logger);
			//
			//			List<Field> ipSet11 = null;
			//			try {
			//				ipSet11 = trans.apply().getRowSets().get(0).getFields();
			//
			//			} catch (InvalidTransformationException e) {
			//				e.printStackTrace();
			//			}
			//			Iterator<Field> iter1 = ipSet11.iterator();
			//
			//			logger.debug("Found " + ipSet11.size() + " output fields");
			//			List<Field> fields = new ArrayList<Field>();
			//			logger.debug("Validate output Field");
			//
			//			while (iter1.hasNext()) {
			//
			//				Field isetElement = iter1.next();
			//
			//				fields.add(isetElement);
			//
			//			}
			//			validateFieldList(fields);

		}

	}

	public static void validateInputSet(InputSet isetElement) {
		List<IOutputField> outFields = isetElement.getOutputFields();
		List<Field> fields = new ArrayList<Field>();
		for (IOutputField tField : outFields) {

			fields.add(tField.getField());
		}
		validateFieldList(fields);

		List<PortDef> pdList = isetElement.getPortDefs();

		for (PortDef pd : pdList) {
			printConnectorInfo(pd);

		}
	}

	public static void printConnectorInfo(PortDef pd) {
		String fromInst = pd.getFromInstanceName();
		int fromInstType = pd.getFromInstanceType();

		String toInst = pd.getToInstanceName();
		int toInstType = pd.getToInstanceType();

		Field fromField = pd.getInputField();
		Field toField = pd.getOutputField();
		String fromFieldName = fromField.getName();
		String fromFieldType = fromField.getDataType();
		String fromFieldPrecision = fromField.getPrecision();
		String fromFieldScale = fromField.getScale();
		String toFieldName = toField.getName();
		String toFieldType = toField.getDataType();
		String toFieldPrecision = toField.getPrecision();
		String toFieldScale = toField.getScale();

		String connectorInfo = "Instance " + fromInst + "(type : " + fromInstType + ") field " + fromFieldName + " ( "
				+ fromFieldType + " [ " + fromFieldPrecision + " , " + fromFieldScale + " ] ) ===> ";
		connectorInfo = connectorInfo + "Instance " + toInst + "(type : " + toInstType + ") field " + toFieldName
				+ " ( " + toFieldType + " [ " + toFieldPrecision + " , " + toFieldScale + " ] )";

		logger.debug(connectorInfo);
	}

	public static void debugSQLTransMetaExtension(SQLTransformation sqlTrans) {
		HelperBase.printSplitLine(logger);
		logger.debug("SQL trans metadata ");
		HelperBase.printSplitLine(logger);

		sqlTrans.setMetaExtensionValue("DB_Type", "2");

		List<MetaExtension> metas = sqlTrans.getMetaExtensions();

		for (MetaExtension meta : metas) {
			logger.debug("SQL transform metaextension " + meta.getExtensionName() + " : value (" + meta.getValue()
					+ ")");
		}

		logger.debug("SQL trans database type : " + sqlTrans.getDBType().toString());
	}

	public static void auditTargetConnectionInfo(Target tgt) {
		logger.debug("...Get target object : " + tgt.getName());

		logger.debug("Audit the target connection info : " + tgt.getConnInfo().toString());
		for (ConnectionInfo cinfo : tgt.getConnInfos()) {
			for (Object key : cinfo.getConnProps().keySet()) {
				logger.debug("con key : " + key.toString() + " --> value : "
						+ cinfo.getConnProps().getProperty((String) key));
			}
		}
	}

	public static void validateTransformInstance(Transformation trans) {

		HelperBase.printSplitLine(logger);
		logger.debug("Validate transform " + trans.getName() + " TransFields Attributes");
		HelperBase.printSplitLine(logger);

		List<TransformFieldAttrDefinition> TransAttrDefs = trans.getTransformationFieldAttrs();
		if (TransAttrDefs == null) {
			logger.error("There is no transform fields attributes list");
		} else {
			Iterator<TransformFieldAttrDefinition> iter = TransAttrDefs.iterator();
			while (iter.hasNext()) {
				TransformFieldAttrDefinition ta = (TransformFieldAttrDefinition) iter.next();
				logger.debug(ta.getName() + " , " + ta.getDataType() + " , order : " + ta.getOrder() + " , DefType : "
						+ ta.getTransFldAttrDefType());
			}
		}

		HelperBase.printSplitLine(logger);
		logger.debug("Validate the group sets");

		List<GroupSet> transGroupSets = trans.getTransContext().getGroupSets();

		logger.debug("...There are " + transGroupSets.size() + " group set");
		for (GroupSet tGroup : transGroupSets) {
			logger.debug("......Group set " + tGroup.getName() + " is " + tGroup.getGroupType() + " type");

			List<Field> outFields = tGroup.getOutputFields();

			validateFieldList(outFields);
		}

		HelperBase.printSplitLine(logger);
		logger.debug("Validate the input sets");
		List<InputSet> ipSet = trans.getTransContext().getInputSets();
		for (InputSet ip : ipSet) {
			RowSet iRS = ip.getInRowSet();
			if (iRS != null) {
				logger.debug("...Validate tranform context inputset InRowSet");
				validateFieldList(iRS.getFields());
			} else {
				logger.debug("There is no tranform context inputset InRowSet");
			}
		}
		for (InputSet ip : ipSet) {
			RowSet oRS = ip.getOutRowSet();

			if (oRS != null) {
				logger.debug("...Validate tranform context inputset OutRowSet");
				validateFieldList(oRS.getFields());
			} else {
				logger.debug("There is no tranform context inputset OutRowSet");
			}
		}

		HelperBase.printSplitLine(logger);
		logger.debug("...Validate tranform context GroupRowSet");

		RowSet rs = trans.getTransContext().getGroupRowSet();
		if (rs != null) {
			validateFieldList(rs.getFields());
		} else {
			logger.debug("There is no group rowset");
		}

	}

	public static void validateTransformProperties(TransformationProperties props) {

		HelperBase.printSplitLine(logger);
		logger.debug("Validate transform properties ");

		if (props != null && props.size() > 0) {
			logger.debug("Start validating");

			for (Object key : props.keySet()) {
				logger.trace("... Find key : " + key.toString());

				String value = props.getProperty(key.toString());
				logger.trace("\t\t The value of this key : '" + value + "'");
			}
		} else {
			logger.error("There is no property");
		}

	}
}
