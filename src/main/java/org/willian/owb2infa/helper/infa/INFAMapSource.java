package org.willian.owb2infa.helper.infa;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.InputSet;
import com.informatica.powercenter.sdk.mapfwk.core.Mapping;
import com.informatica.powercenter.sdk.mapfwk.core.OutputSet;
import com.informatica.powercenter.sdk.mapfwk.core.RowSet;
import com.informatica.powercenter.sdk.mapfwk.core.ShortCut;
import com.informatica.powercenter.sdk.mapfwk.core.Source;
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;

public class INFAMapSource {

	private Logger logger = LogManager.getLogger(INFAMapSource.class);

	public void addSource(MetadataOperator op, Folder folder, Repository rep,
			Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String opName = op.getName();

		String opBoundName = op.getBoundName();

		if (opBoundName == null) {
			opBoundName = opName;
		}

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add source " + opName + " with bound table "
				+ opBoundName);
		HelperBase.printSplitLine(logger);

		// logger.debug("Check and add shortcut if the source " + opBoundName
		// + " is existed in SHARED folder (OWB operator : " + opName
		// + ")");

		if (!INFAUtils.isLoadControlTable(opBoundName)) {

			RowSet boundRS = expInputSetHM.get(opBoundName);

			if (boundRS == null) {

				ShortCut sourceShortCut = this.getShortCutFromFolder(folder,
						opBoundName);

				if (sourceShortCut != null) {

					addShortCutSourceInMapping(op, sourceShortCut, folder,
							mapping, owbMapping, expInputSetHM);

				} else {
					logger.debug("Can't find the shortcut for source " + opName
							+ " in folder " + folder.getName()
							+ ". Check or add the source " + opName
							+ " in local folder");
					addIfNotExistedSource(op, folder, rep, mapping, owbMapping,
							expInputSetHM);
				}
			}
		} else {
			logger.debug("Skip the load control table because it has been deprecated in Informatica");
		}
	}

	public void addIfNotExistedSource(MetadataOperator sourceOP, Folder folder,
			Repository rep, Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String sourceName = sourceOP.getBoundName();

		if (sourceName == null) {
			sourceName = sourceOP.getName();
		}
		Source src = folder.getSource(sourceName);
		if (src != null) {
			if (!INFAUtils.isLoadControlTable(sourceName)) {

				boolean found = checkSourceInMap(sourceOP, mapping);

				if (!found) {
					logger.debug("SOURCE " + sourceName
							+ " is NOT existed in mapping");
					constructDSQTrans(sourceOP, src, folder, mapping,
							owbMapping, expInputSetHM);
				}
			} else {
				logger.debug("Found "
						+ sourceName
						+ " as source in OWB mapping. It has been deprecated in Informatica");
			}
		} else {
			logger.error("Can't retrieve source " + sourceName + " in folder "
					+ folder.getName());
		}
	}

	private void addShortCutSourceInMapping(MetadataOperator operator,
			ShortCut scSource, Folder folder, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		String sourceOPName = operator.getName();

		String scName = scSource.getName();
		boolean replaceNextTransAsDSQ = false;
		Source refSource = (Source) scSource.getRefObject();
		String sourceName = refSource.getName();

		logger.debug("Add shortcut " + scName + " into Mapping as source "
				+ sourceOPName);

		logger.debug("Retrieve operator " + sourceOPName
				+ " from owb mapping object");
		HashSet<String> connectTo = operator.getOutgoingRelationships();

		for (String connectToOP : connectTo) {
			String connectOPName = INFAUtils.extractFirstPart(connectToOP);

			MetadataOperator connectOP = owbMapping
					.getOperatorByName(connectOPName);
			if (connectOP != null
					&& INFAUtils.isTransformReplacedWithDSQ(owbMapping,
							connectOP)) {
				replaceNextTransAsDSQ = true;
			}

		}

		if (replaceNextTransAsDSQ) {

			logger.debug("The "
					+ sourceName
					+ " connects to joiner, so skip the construction of the DSQ for the source "
					+ sourceName);

			addSourceOnlyIntoHashMap(operator, refSource, expInputSetHM);

			// duplicate source in case we use rowset not the source to create
			// the inputset.
			// mapping.addShortCut(scSource);
//			mapping.addSource(refSource);

		} else {

			addShortCutAndDSQIntoMap(scSource, mapping, expInputSetHM);
		}

		mapping.addShortCut(scSource);
	}

	private void constructDSQTrans(MetadataOperator sourceOP, Source src,
			Folder folder, Mapping mapping, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		String sourceOPName = sourceOP.getName();
		boolean replaceNextTransAsDSQ = false;

		logger.debug("Retrieve operator " + sourceOPName
				+ " from owb mapping object");
		HashSet<String> connectTo = sourceOP.getOutgoingRelationships();

		for (String connectToOP : connectTo) {
			String connectOPName = INFAUtils.extractFirstPart(connectToOP);

			MetadataOperator connectOP = owbMapping
					.getOperatorByName(connectOPName);
			if (connectOP != null
					&& INFAUtils.isTransformReplacedWithDSQ(owbMapping,
							connectOP)) {
				replaceNextTransAsDSQ = true;
			}

		}

		// debug
		// connectToJoiner = false;

		if (replaceNextTransAsDSQ) {
			logger.debug("The "
					+ sourceOPName
					+ " connects to a transform which can be replaced as a DSQ, so skip the construction of the DSQ for the source "
					+ sourceOPName);
			addSourceOnlyIntoHashMap(sourceOP, src, expInputSetHM);

//			mapping.addSource(src);

		} else {
			addSourceAndDSQIntoMap(sourceOP, src, folder, mapping,
					expInputSetHM);
		}
	}

	public void addSourceOnlyIntoHashMap(MetadataOperator operator,
			Source refSource, HashMap<String, RowSet> expInputSetHM) {

		String sourceName = refSource.getName();

		// String bizName = refSource.getBusinessName();

		String identifier = sourceName;

		InputSet expInput = new InputSet(refSource);
		RowSet outRowSet = expInput.getOutRowSet();
		outRowSet.setName(sourceName);
		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, outRowSet);
		logger.debug("Add identifier " + identifier + " into memory");

	}

	public void addShortCutAndDSQIntoMap(ShortCut scSource, Mapping mapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {

		String sourceOPName = scSource.getName();

		logger.debug("Get the Source form Shortcut " + sourceOPName
				+ " and invoke the method addSourceAndDSQIntoMap");
		Source refSource = (Source) scSource.getRefObject();
		String sourceName = refSource.getName();

		logger.debug(" Create the DSQ for " + sourceOPName);

		String transName = INFAPropConstants.getINFATransPrefix("SOURCE")
				+ INFAUtils.toCamelCase(sourceOPName);
		String identifier = INFAPropConstants.getINFATransPrefix("SOURCE")
				+ sourceName;

		Transformation dsqTrans = refSource.createDSQTransform();

		dsqTrans.setName(transName);
		dsqTrans.setBusinessName(sourceOPName);
		dsqTrans.setInstanceName(transName);

		String sourceFilter = SQLProcessor.generateSourceFilter();

		if (sourceFilter == null) {
			logger.error("The source filter for operator " + transName
					+ " is empty");
		} else {
			dsqTrans.getTransformationProperties().setProperty("Source Filter",
					sourceFilter);
			logger.debug("The source filter is : "
					+ dsqTrans.getTransformationProperties().getProperty(
							"Source Filter"));
		}

		mapping.addTransformation(dsqTrans);

		logger.debug("Add DSQ instance " + identifier + " into mapping");

		OutputSet outSet = dsqTrans.apply();

		RowSet expRowSet = outSet.getRowSets().get(0);

		logger.debug("Add identifier " + identifier
				+ " for the rowset of source " + sourceName);
		INFAUtils.addToExpRowSetHM(expInputSetHM, sourceName, expRowSet);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, expRowSet);

		logger.debug("Add SOURCE " + sourceOPName + " as the identifier "
				+ identifier + " and its DSQ transform to memory hashmap");

	}

	public void addSourceAndDSQIntoMap(MetadataOperator sourceOP,
			Source refSource, Folder folder, Mapping mapping,
			HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException {
		String sourceOPName = sourceOP.getName();
		String sourceName = refSource.getName();
		String transName = INFAPropConstants.getINFATransPrefix("SOURCE")
				+ INFAUtils.toCamelCase(sourceOPName);
		String identifier = INFAPropConstants.getINFATransPrefix("SOURCE")
				+ sourceName;

		logger.debug("The " + sourceOPName
				+ " doesn't connect to joiner, so create the DSQ for "
				+ sourceOPName);

		Transformation dsqTrans = refSource.createDSQTransform();

		dsqTrans.setName(transName);
		dsqTrans.setBusinessName(sourceOPName);
		dsqTrans.setInstanceName(transName);

		String sourceFilter = SQLProcessor.generateSourceFilter();

		if (sourceFilter == null) {
			logger.error("The source filter for operator " + transName
					+ " is empty");
		} else {
			dsqTrans.getTransformationProperties().setProperty("Source Filter",
					sourceFilter);
			logger.debug("The source filter is : "
					+ dsqTrans.getTransformationProperties().getProperty(
							"Source Filter"));
		}

		logger.debug("Add DSQ instance " + identifier + " into mapping");

		OutputSet outSet = dsqTrans.apply();

		RowSet expRowSet = outSet.getRowSets().get(0);

		mapping.addTransformation(dsqTrans);

		logger.debug("Add identifier " + identifier
				+ " for the rowset of source " + sourceName);
		INFAUtils.addToExpRowSetHM(expInputSetHM, sourceName, expRowSet);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, expRowSet);

		logger.debug("Add SOURCE " + sourceOPName + " as the identifier "
				+ identifier + " and its DSQ transform to memory hashmap");

	}

	private boolean checkSourceInMap(MetadataOperator sourceOP, Mapping mapping) {

		String sourceName = sourceOP.getName();

		boolean found = false;
		List<Source> sources = mapping.getSources();
		Iterator<Source> sourceIter = sources.iterator();
		while (sourceIter.hasNext()) {
			Source source = sourceIter.next();

			String srcName = source.getName();
			String srcInstName = source.getInstanceName();
			if (srcName.equals(sourceName) || srcInstName.equals(sourceName)) {
				found = true;
			}
		}

		if (found) {
			logger.debug("Source " + sourceName + " is existed in mapping");
		}
		return found;
	}

	private ShortCut getShortCutFromFolder(Folder toCheckFolder,
			final String checkName) {
		final String scName = checkName;
		logger.debug("Try to get shortcut " + scName + " from folder "
				+ toCheckFolder.getName());

		// shortcuts are created under local folder. so don't need to fetch from
		// repository
		List<ShortCut> scList = toCheckFolder.getShortCuts();

		if (scList != null && scList.size() > 0) {

			for (ShortCut sc : scList) {
				String name = sc.getName();
				logger.debug("...Checking shortcut " + name);

				if (name.equalsIgnoreCase(scName)) {
					logger.debug("Found shortcut " + name);
					return sc;
				}
			}

		} else {
			logger.debug("Can't find the shortcut " + scName);
		}
		return null;

	}
}
