package org.willian.owb2infa.helper.infa;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariable;
import com.informatica.powercenter.sdk.mapfwk.core.MappingVariableDataTypes;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.Session;
import com.informatica.powercenter.sdk.mapfwk.core.SessionPropsConstants;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Workflow;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;
import com.thoughtworks.xstream.XStream;

public class INFAMapConstructor extends Base {

	MetadataMapping owbMapping;
	HashMap<String, RowSet> expInputSetHM = new HashMap<String, RowSet>();

	private Logger logger = LogManager.getLogger(INFAMapConstructor.class);

	public INFAMapConstructor(String xmlDir, String outputDir,
			String mappingName) throws IOException {
		super();

		infaDir = outputDir;
		XStream xstream = new XStream();

		String owbMetadataXML = xmlDir + java.io.File.separatorChar
				+ mappingName + ".xml";

		logger.debug("Ready to parse " + owbMetadataXML);

		InputStream metadataStream = getInputStream(xmlDir, mappingName
				+ ".xml");

		xstream.processAnnotations(MetadataMapping.class);
		owbMapping = (MetadataMapping) xstream.fromXML(metadataStream);

		logger.debug(owbMapping.getBusiness_name()
				+ " has been deserialized back from " + owbMetadataXML);

		metadataStream.close();

	}

	@Override
	protected void createSession() throws Exception {
		String owbMappingName = owbMapping.getBusiness_name();
		String targetName = owbMappingName.replace("_MAP", "");
		String sessionName = "s_" + INFAUtils.toCamelCase(targetName);
		String sessionBusName = "Session_For_" + owbMappingName;
		String sessionDesc = "This is session for " + owbMappingName;
		session = new Session(sessionName, sessionBusName, sessionDesc);
		session.setMapping(this.mapping);

		Properties sesProp = this.session.getProperties();
		sesProp.setProperty(SessionPropsConstants.SOURCE_CONNECTION_VALUE,
				"$DBConnection_SourceEcommReplica_Buy");
		sesProp.setProperty(SessionPropsConstants.TARGET_CONNECTION_VALUE,
				"$DBConnection_TargetDW");

	}

	@Override
	protected void createWorkflow() throws Exception {
		String owbMappingName = owbMapping.getBusiness_name();
		String targetName = owbMappingName.replace("_MAP", "");
		String wfName = "wf_" + INFAUtils.toCamelCase(targetName);
		String wfBusName = "Workflow_For_" + owbMapping.getName();
		String wfDesc = "This is workflow for " + owbMapping.getName();

		workflow = new Workflow(wfName, wfBusName, wfDesc);
		workflow.addSession(session);
		folder.addWorkFlow(workflow);

	}

	@Override
	protected void createSources() {

		ArrayList<MetadataOperator> operators = owbMapping.getOperators();
		for (MetadataOperator operator : operators) {

			boolean isSource = operator.isSource();

			if (isSource) {
				String opName = operator.getName();

				logger.debug("Ready to invoke object INFASource to create source "
						+ opName + " in folder " + folderName);

				INFASource infaSource = new INFASource();

				infaSource.createSource(operator, folder, rep);
			}
		}

	}

	@Override
	protected void createTargets() {
		ArrayList<MetadataOperator> operators = owbMapping.getOperators();
		for (MetadataOperator operator : operators) {

			boolean isTarget = operator.isTarget();

			if (isTarget) {
				String opName = operator.getName();

				logger.debug("Ready to invoke object INFATarget to create target "
						+ opName + " in folder " + folderName);

				INFATarget infaTarget = new INFATarget();

				infaTarget.createTarget(operator, folder, rep);
			}
		}

	}

	@Override
	protected void createMappings() throws Exception {

		// The owb mapping business name is more accurate. So we use it for
		// informatica mapping name.
		// String owbMappingName = owbMapping.getName();
		String owbMappingName = owbMapping.getBusiness_name();

		String targetName = owbMappingName.replace("_MAP", "");
		String infaMappingName = "m_src_to_tgt_"
				+ INFAUtils.toCamelCase(targetName);
		String infaMappingBusName = owbMapping.getBusiness_name();
		String infaMappingDesc = "It's migrated from OWB mapping "
				+ owbMappingName;

		mapping = new Mapping(infaMappingName, infaMappingBusName,
				infaMappingDesc);

		setMapFileName(mapping);

		mapping.addMappingVariable(new MappingVariable("",
				MappingVariableDataTypes.TIMESTAMP, "", "", true,
				"$$read_src_data_from_date", "29", "9", true));

		constructMapFromSource();

		// folder.addMapping(mapping);

		List<ShortCut> shortcuts = folder.getShortCuts();

		for (ShortCut sc : shortcuts) {

			logger.debug("After create mapping, Find shortcut in local folder : "
					+ sc.getName() + " (folder name : " + sc.getFolderName());
		}

		// INFAValidator.doValidation(rep, mapping);

	}

	private void constructMapFromSource()
			throws InvalidTransformationException, RepoOperationException,
			MapFwkReaderException {

		addSources();

		addTransforms();

		addTargets();

	}

	private void addSources() throws InvalidTransformationException {
		ArrayList<MetadataOperator> owbOps = owbMapping.getOperators();
		for (MetadataOperator op : owbOps) {
			if (op.isSource()) {
				logger.debug("Ready to invoke object INFAMapSource to add source "
						+ op.getName() + " in mapping");

				INFAMapSource infaMapSource = new INFAMapSource();

				infaMapSource.addSource(op, folder, rep, mapping, owbMapping,
						expInputSetHM);
			}

		}
	}

	private void addTransforms() throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		List<MetadataOperator> transOPs = owbMapping.getTransformOperators();

		for (MetadataOperator op : transOPs) {
			logger.debug("Ready to invoke INFAMapTransform to add Transformation "
					+ op.getName() + " in mapping");
			INFAMapTransform infaMapTrans = new INFAMapTransform();
			infaMapTrans.addIfNotExistedTransform(op, folder, rep, mapping,
					owbMapping, expInputSetHM);
		}
	}

	private void addTargets() throws InvalidTransformationException {

		ArrayList<MetadataOperator> owbOps = owbMapping.getOperators();
		for (MetadataOperator op : owbOps) {

			if (op.isTarget()) {
				logger.debug("Ready to invoke object INFAMapTarget to add target "
						+ op.getName() + " in mapping");

				INFAMapTarget infaMapTarget = new INFAMapTarget();
				infaMapTarget.constructTargetInMap(op, folder, mapping,
						owbMapping, expInputSetHM);
			}
		}

	}

}
