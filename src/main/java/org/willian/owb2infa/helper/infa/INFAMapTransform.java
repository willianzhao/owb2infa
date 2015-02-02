package org.willian.owb2infa.helper.infa;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.TreeSet;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.informatica.powercenter.sdk.mapfwk.core.AggregateTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.ExpTransformation;
import com.informatica.powercenter.sdk.mapfwk.core.Field;
import com.informatica.powercenter.sdk.mapfwk.core.FieldType;
import com.informatica.powercenter.sdk.mapfwk.core.Folder;
import com.informatica.powercenter.sdk.mapfwk.core.INameFilter;
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
import com.informatica.powercenter.sdk.mapfwk.core.Transformation;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationConstants;
import com.informatica.powercenter.sdk.mapfwk.core.TransformationContext;
import com.informatica.powercenter.sdk.mapfwk.core.UnionTransformation;
import com.informatica.powercenter.sdk.mapfwk.exception.InvalidTransformationException;
import com.informatica.powercenter.sdk.mapfwk.exception.MapFwkReaderException;
import com.informatica.powercenter.sdk.mapfwk.exception.RepoOperationException;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortLinkContextFactory;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContext;
import com.informatica.powercenter.sdk.mapfwk.portpropagation.PortPropagationContextFactory;
import com.informatica.powercenter.sdk.mapfwk.repository.Repository;
import com.informatica.powercenter.sdk.mapfwk.repository.RepositoryObjectConstants;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.helper.owb.OWBPropConstants;
import org.willian.owb2infa.model.MetadataConnection;
import org.willian.owb2infa.model.MetadataField;
import org.willian.owb2infa.model.MetadataGroup;
import org.willian.owb2infa.model.MetadataMapping;
import org.willian.owb2infa.model.MetadataOperator;
import org.willian.owb2infa.model.MetadataProperty;

import edu.emory.mathcs.backport.java.util.Arrays;

public class INFAMapTransform {

	static Logger logger = LogManager.getLogger(INFAMapTransform.class);
	TransformHelper helper = null;

	public void addIfNotExistedTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		String transName = transOP.getName();
		boolean found = checkTransformInMap(transName, mapping);

		if (!found) {

			helper = new TransformHelper(mapping);

			logger.debug("Transformation " + transName + " is NOT existed in mapping. Ready to process and add it.");

			String opType = transOP.getType();
			logger.debug("Identify the transformation type as " + opType);

			if (INFAUtils.isSupportedOWBTransform(opType)) {
				switch (opType) {
					case "JOINER":
						logger.debug("Add joiner transform");
						addJoinerTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "FILTER":
						logger.debug("Add Filter transform");
						addFilterTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "LOOKUP":
						logger.debug("Add Lookup transform");
						addLookupTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "EXPRESSION":
						logger.debug("Add Expression transform");
						addExpressTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "SET_OPERATION":
						logger.debug("Add Union transform");
						addUnionTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "AGGREGATOR":
						logger.debug("Add Aggregator transform");
						addAggrTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "TRANSFORMATION":
						logger.debug("Add Store Procedure transform");
						addFunctionTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "UNPIVOT":
						logger.debug("Add transforms for OWB unpivot");
						addTransformForUnpivot(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "SORTER":
						logger.debug("Add SORTER transform");
						addSorterTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "SPLITTER":
						logger.debug("Add SPLITTER transform");
						addRouterTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "POSTMAPPING_PROCESS":
						logger.debug("Add Store Procedure transform for post mapping");
						addPostMappingSP(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					case "PREMAPPING_PROCESS":
						logger.debug("Add Store Procedure transform for pre mapping");
						addPreMappingSP(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;

					default:
						logger.debug("Add Dummy transform for unsupported operator");
						addDummyTransformForPlaceHolder(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
				}
			} else {
				switch (opType) {
					case "CONSTANT":
						logger.debug(opType + " " + transName + " will be handled specially");
						break;
					case "SEQUENCE":
						logger.debug("Add Sequence transform");

						logger.debug(opType + " " + transName
								+ " will be added as store procedure transformation and embeded into expression ");
						addSequenceTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
						break;
					default:
						logger.error(opType + " " + transName + " is not supported yet");
				}
			}

		}
	}

	private boolean checkTransformInMap(String transName, Mapping mapping) {
		boolean found = false;
		List<Transformation> transformations = mapping.getTransformations();
		logger.debug("Check if the transformation is existed in mapping");
		logger.debug("There are " + transformations.size() + " transform in mapping " + mapping.getName());

		Iterator<Transformation> transIter = transformations.iterator();
		while (transIter.hasNext()) {
			Transformation trans = transIter.next();
			// logger.debug("Found transformation " + trans.getName() +
			// "in mapping " + mapping.getName());
			/*
			 * get the required transformation
			 */
			if (trans.getName().equals(transName)) {
				found = true;

				logger.debug("Transformation " + transName + " is existed in mapping");

			}

		}

		return found;
	}

	public void addJoinerTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Joiner transform");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();
		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		boolean replaceTransAsDSQ = INFAUtils.isTransformReplacedWithDSQ(owbMapping, transOP);

		if (replaceTransAsDSQ) {

			logger.debug("There is no Transform connecting to this JOINER hence a common DSQ is created to replace the joiner");

			try {
				createDSQForSourceJoiner(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
			} catch (Exception e) {
				logger.debug("Construct with the real join transform");
				createJoinerTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
			}
		} else {

			logger.debug("Construct with the real join transform");
			createJoinerTransform(transOP, folder, rep, mapping, owbMapping, expInputSetHM);
		}
	}

	private void createDSQForSourceJoiner(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		logger.debug("Ready to construct Common DSQ transformation for this joiner");
		HelperBase.printSplitLine(logger);

		List<InputSet> inputSets = new ArrayList<InputSet>();

		String transName = transOP.getName();

		checkAndInstallAllRelatesUptoSource(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for joiner " + transName
				+ " have been processed and added into export set");

		transName = INFAUtils.toCamelCase(transName);

		String outputGroupName = transOP.getOutputGroupNames().get(0);
		String joinerIdentifier = transOP.getName() + "." + outputGroupName;

		Iterator<String> inIdentifierIter;
		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		inIdentifierIter = incomingRelatedOPs.iterator();
		while (inIdentifierIter.hasNext()) {
			String inIdentifier = inIdentifierIter.next();
			String inOPName = INFAUtils.extractFirstPart(inIdentifier);

			if (!INFAUtils.isLoadControlTable(inOPName)) {

				MetadataOperator inOP = owbMapping.getOperatorByName(inOPName);

				if (inOP.isSource()) {

					logger.trace("This is to prevent <ASSOCIATED_SOURCE_INSTANCE/> error");

					logger.trace("The memory hashmap has following entries for DSQ transform of joiner :");
					for (String key : expInputSetHM.keySet()) {
						logger.trace("\t" + key);
					}

					String inOPBoundName = inOP.getBoundName();

					if (inOPBoundName == null) {
						inOPBoundName = inOP.getName();
					}

					Source checkSource = INFASource.getSourceFromShareFolder(rep, inOPBoundName, "*");

					if (checkSource == null) {
						// ------------- rowset will be used for soure created
						// in local folder
						// logger.trace("Use the rowset of local source to construct inputset list of Joiner DSQ");
						//
						// InputSet inSet = null;
						// RowSet expRowSet = null;
						// expRowSet = INFAUtils.getExpRowSet(expInputSetHM,
						// inOPBoundName);
						//
						// if (expRowSet != null) {
						//
						// logger.trace("Get memory rowset " + inOPBoundName);
						//
						// inSet = new InputSet(expRowSet);
						//
						// inputSets.add(inSet);
						// logger.debug("Add " + inOPName +
						// " as one of the inputsets of joiner");
						// }

						checkSource = INFASource.getSoureFromFolder(folder, inOPBoundName, "*");

					} else {
						logger.debug("Add the shortcut referenced source (" + inOPBoundName + ") into mapping");
						mapping.addSource(checkSource);
					}

					if (checkSource != null) {
						// ----------- inputset(Source) will be used for the
						// shortcut from shard folder

						logger.trace("Use the source (" + checkSource.getName()
								+ ") to construct inputset list of Joiner DSQ");

						// List<Source> sources = mapping.getSources();

						// for (Source source : sources) {
						// String sourceName = source.getName();
						//
						// logger.trace("...Get source " + sourceName);
						//
						// if (sourceName.equals(inOPBoundName)) {
						InputSet joinerInput = new InputSet(checkSource);

						inputSets.add(joinerInput);

						logger.debug("Add " + inOPName + " as one of the inputsets of joiner");
						// }
						// }
					}

				} else {
					HashSet<String> incomingRelationships = inOP.getIncomingRelationships();
					if (incomingRelationships != null && incomingRelationships.size() == 0) {

						logger.debug("The connection operator " + inOPName
								+ " will be removed and moved to DSQ source filter property.");
					}
				}

			} else {
				logger.debug("Skip to add " + inOPName + " into the input set for Joiner");
			}
		}

		String joinerName = INFAPropConstants.getINFATransPrefix("JOINER") + transName;

		Transformation joinerTransform = null;

		TransformationContext tc = null;

		tc = new TransformationContext(inputSets);

		joinerTransform = tc.createTransform(TransformationConstants.DSQ, joinerName);

		String businessName = transName;
		joinerTransform.setBusinessName(businessName);

		logger.debug("Construct DSQ " + joinerTransform.getName() + " with business name "
				+ joinerTransform.getBusinessName());

		String joinCondition = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, null);
		String sourceFilter = SQLProcessor.generateSourceFilter();

		joinCondition = SQLProcessor.removeLoadControlClause(owbMapping, joinCondition);

		if (joinCondition == null) {
			logger.error("The join condition for operator " + transName + " is empty");
		} else {
			joinerTransform.getTransformationProperties().setProperty("User Defined Join", joinCondition);
			logger.debug("The join condition is : "
					+ joinerTransform.getTransformationProperties().getProperty("User Defined Join"));
		}

		if (sourceFilter == null) {
			logger.error("The source filter for operator " + transName + " is empty");
		} else {
			joinerTransform.getTransformationProperties().setProperty("Source Filter", sourceFilter);
			logger.debug("The source filter is : "
					+ joinerTransform.getTransformationProperties().getProperty("Source Filter"));
		}

		OutputSet outSet = joinerTransform.apply();
		RowSet joinRS = outSet.getRowSets().get(0);

		mapping.addTransformation(joinerTransform);
		logger.debug("Joiner " + transName + "(" + joinerIdentifier + ") has been added as a DSQ transform.");

		addNamingExpressionAfterJoiner(transName, joinRS, transOP, mapping, expInputSetHM);

		HelperBase.printSplitLine(logger);
	}

	/*
	 * Depends on the type of operators connecting to joiner operator, if the
	 * operator is load control, then it will be skipped. Exclude the load
	 * control operator, if only two operators are connecting to joiner, and one
	 * of them are isolated operator, like constant, expression, then a FILTER
	 * transform will be used (not implement now). If there are two or more
	 * operators exclude the load control and isolated operator, then a serial
	 * of JOINER transforms will be used. In the output group of OWB joiner
	 * operator, if the output field name is different from the expression field
	 * reference, then an expression transform will be created after the joiner
	 * for field mapping. For sorted input, a sorter will be used before every
	 * input of joiner
	 */
	private void createJoinerTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		String opName = transOP.getName();

		HelperBase.printSplitLine(logger);
		logger.debug("Deal with the join transform " + opName);
		HelperBase.printSplitLine(logger);

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for Joiner " + opName
				+ " have been processed and added into export set");

		Hashtable<String, INFAInputSetUnit> allValidInputs = new Hashtable<String, INFAInputSetUnit>();

		HashMap<String, String> replaceClause = new HashMap<String, String>();

		// // TransformHelper helper = new TransformHelper(mapping);

		logger.debug("Prepare the queue for input transfroms");
		Queue<String> tableQueue = new LinkedList<String>();
		Queue<String> transformQueue = new LinkedList<String>();

		ArrayList<MetadataGroup> allGroups = transOP.getGroups();
		for (MetadataGroup group : allGroups) {

			String groupName = group.getName();
			if (group.getDirection().matches(".*IN.*")) {

				// Input group

				HashSet<String> upwardConnNames = group.getUpwardConnectionNames();

				if (upwardConnNames.size() == 1) {

					// 1:1 group

					logger.debug("Only one upward operator connects to this group " + groupName);
					boolean skipGroup = false;
					String inIdentifier = null;
					RowSet inRS = null;

					Iterator<String> itConnNames = upwardConnNames.iterator();

					String inConnName = itConnNames.next();

					String inOPName = INFAUtils.extractFirstPart(inConnName);
					String inGroup = INFAUtils.extractSecondPart(inConnName);

					MetadataOperator mOP = owbMapping.getOperatorByName(inOPName);

					String boundName = mOP.getBoundName();
					if (boundName == null) {
						boundName = inOPName;
					}

					replaceClause.put(groupName, inOPName);

					if (mOP.isSource()) {
						if (!INFAUtils.isLoadControlTable(boundName)) {
							inIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE") + inOPName;
							tableQueue.offer(inIdentifier);
							logger.debug("Find the source " + inIdentifier + " as the input of Joiner");
						} else {
							skipGroup = true;
							logger.debug("Skip the load control table for the Joiner");
						}
					} else {
						HashSet<String> incomingRelationships = mOP.getIncomingRelationships();
						if (incomingRelationships != null && incomingRelationships.size() > 0) {
							inIdentifier = inOPName + "." + inGroup;
							transformQueue.offer(inIdentifier);
							logger.debug("Find the operator " + inIdentifier + " as the input of Joiner");
						} else {
							skipGroup = true;

							logger.debug("Skip the standalone operator " + inOPName + " for the group");
						}
					}

					ArrayList<MetadataField> mFields = group.getFields();

					for (MetadataField mField : mFields) {

						ArrayList<MetadataConnection> inConns = mField.getInputConnections();
						if (inConns != null) {
							MetadataConnection inConn = inConns.get(0);
							String inField = inConn.getSourceField();

							if (!skipGroup) {
								if (inRS == null) {
									INFAInputSetUnit tempUnit = new INFAInputSetUnit();
									inRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);
									tempUnit.setRs(inRS);
									allValidInputs.put(inIdentifier, tempUnit);

									logger.debug("Initialize the rowset for " + inIdentifier);
								}

								if (allValidInputs.get(inIdentifier).getRs() != null) {

									Field iField = inRS.getField(inField);

									Field tField = INFAUtils.convertToField(mField, FieldType.TRANSFORM);

									if (iField != null && tField != null) {
										allValidInputs.get(inIdentifier).getExprLink().put(iField, tField);
										allValidInputs.get(inIdentifier).getIncludeFields().add(iField);

										logger.debug("Link upward field " + iField.getName() + " to target field "
												+ tField.getName());

									} else if (iField == null) {
										logger.debug("Can't find " + inField + " in the upward Rowset of "
												+ inIdentifier);
									} else if (tField == null) {
										logger.debug("The field " + mField + " of " + groupName
												+ " in Joiner is failed to constructed");
									}
								} else {
									logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
								}
							} else {
								replaceClause.put(inField, inField + " -- to remove\n");
							}
						}
					}
				} else if (upwardConnNames.size() > 1) {
					logger.debug("Additional expression transfrom will be created for consolidating the incoming connections for this group "
							+ groupName);

					// N:1 group

					ArrayList<String> effectConnections = new ArrayList<String>();

					Iterator<String> itConnNames = upwardConnNames.iterator();

					String loadControlOPName = "";

					while (itConnNames.hasNext()) {
						String inConnName = itConnNames.next();

						String inOPName = INFAUtils.extractFirstPart(inConnName);
						String inGroup = INFAUtils.extractSecondPart(inConnName);

						MetadataOperator mOP = owbMapping.getOperatorByName(inOPName);

						String boundName = mOP.getBoundName();
						if (boundName == null) {
							boundName = inOPName;
						}

						if (!INFAUtils.isLoadControlTable(boundName)) {
							// Except the load control table , all the incoming
							// operators are effective to the joiner
							effectConnections.add(inOPName + "." + inGroup);
						} else {
							loadControlOPName = inOPName;
						}
					}

					if (effectConnections.size() == 1) {
						// Direct to connect to joiner
						String inIdentifier = null;
						String inConnName = effectConnections.get(0);
						String inOPName = INFAUtils.extractFirstPart(inConnName);
						MetadataOperator mOP = owbMapping.getOperatorByName(inOPName);

						if (mOP.isSource()) {
							inIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE") + inOPName;
							tableQueue.offer(inIdentifier);
							logger.debug("Find the source " + inIdentifier + " as the input of Joiner");
						} else {
							inIdentifier = inConnName;
							transformQueue.offer(inIdentifier);
							logger.debug("Find the operator " + inIdentifier + " as the input of Joiner");
						}

						INFAInputSetUnit tempUnit = new INFAInputSetUnit();
						RowSet inRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);
						tempUnit.setRs(inRS);
						allValidInputs.put(inIdentifier, tempUnit);

						logger.debug("Initialize the rowset for " + inIdentifier);

						ArrayList<MetadataField> mFields = group.getFields();

						for (MetadataField mField : mFields) {
							ArrayList<MetadataConnection> inConns = mField.getInputConnections();
							if (inConns != null) {
								MetadataConnection inConn = inConns.get(0);
								String inField = inConn.getSourceField();

								if (allValidInputs.get(inIdentifier).getRs() != null) {

									Field iField = inRS.getField(inField);

									Field tField = INFAUtils.convertToField(mField, FieldType.TRANSFORM);

									if (iField != null && tField != null) {
										allValidInputs.get(inIdentifier).getExprLink().put(iField, tField);
										allValidInputs.get(inIdentifier).getIncludeFields().add(iField);

										logger.debug("Link upward field " + iField.getName() + " to target field "
												+ tField.getName());

									} else if (iField == null) {
										logger.debug("Can't find " + inField + " in the upward Rowset of "
												+ inIdentifier);
									} else if (tField == null) {
										logger.debug("The field " + mField + " of " + groupName
												+ " in Joiner is failed to constructed");
									}
								} else {
									logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
								}

							}
						}

					} else if (effectConnections.size() > 1) {
						// Create additional expression for consolidating
						// incoming fields
						HelperBase.printSplitLine(logger);
						logger.debug("Construct the dummy Expression for joiner group " + groupName);

						String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION")
								+ INFAPropConstants.getINFATransPrefix("JOINER") + groupName;

						ArrayList<InputSet> allInputSetsForGroup = new ArrayList<InputSet>();
						ArrayList<TransformField> outFieldsForDummyExpr = new ArrayList<TransformField>();

						HashSet<String> processedFields = new HashSet<String>();

						Hashtable<String, INFAInputSetUnit> inputsTemp = new Hashtable<String, INFAInputSetUnit>();

						Iterator<MetadataField> itIncludeFieldNames = group.getFields().iterator();

						while (itIncludeFieldNames.hasNext()) {

							MetadataField metadataField = itIncludeFieldNames.next();
							String incomingFieldName = metadataField.getName();

							logger.debug("Processing the field " + incomingFieldName + " in this transform");
							ArrayList<MetadataConnection> inputConnections = metadataField.getInputConnections();

							if (inputConnections != null && inputConnections.size() > 0) {
								MetadataConnection inConnection = inputConnections.get(0);

								String inField = inConnection.getSourceField();
								String inGroup = inConnection.getSourceGroup();
								String inOperator = inConnection.getSourceOperator();

								logger.debug("Processing the incoming " + inOperator + "." + inGroup + "." + inField);

								MetadataOperator mOP = owbMapping.getOperatorByName(inOperator);
								MetadataGroup mGp = mOP.getGroupByName(inGroup);
								MetadataField mFd = mGp.getFieldByName(inField);

								String inIdentifier;
								if (mOP.isSource()) {
									inIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE") + inOperator;
								} else {
									inIdentifier = inOperator + "." + inGroup;
								}

								String expression = mFd.getExpression();
								String opType = mOP.getType();

								if (!processedFields.contains(incomingFieldName)
										&& !inOperator.equals(loadControlOPName)) {
									logger.debug("Ready to add the field " + incomingFieldName);

									Field incomingTransField = INFAUtils.convertToField(metadataField,
											FieldType.TRANSFORM);

									if (INFAUtils.isSupportedOWBTransform(opType)) {
										logger.debug("The incomming connection is from supported operator (type : "
												+ opType + ")");

										if (inputsTemp.get(inIdentifier) == null) {

											logger.debug("Initial InputSet unit instance for in indentifier "
													+ inIdentifier);

											INFAInputSetUnit tempUnit = new INFAInputSetUnit();

											RowSet expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM,
													inIdentifier);

											tempUnit.setRs(expRS);

											inputsTemp.put(inIdentifier, tempUnit);

											logger.debug("Create inputsTemp for in identifier " + inIdentifier);

										}
										if (inputsTemp.get(inIdentifier).getRs() != null) {

											logger.debug("Get export row set for identifier " + inIdentifier);

											logger.debug("Add " + inField + " of " + inGroup + " of " + inOperator
													+ " into transformField list");

											Field sourcingField = inputsTemp.get(inIdentifier).getRs()
													.getField(inField);
											inputsTemp.get(inIdentifier).getIncludeFields().add(sourcingField);

											incomingTransField.setName("IN_" + incomingFieldName);

											inputsTemp.get(inIdentifier).getExprLink()
													.put(sourcingField, incomingTransField);

											logger.debug("-- In Field of Expression transform ("
													+ incomingTransField.getName() + ") is connected from "
													+ sourcingField.getName() + " of " + inGroup + " of " + inOperator);

											Field outputTransField = (Field) incomingTransField.clone();

											outputTransField.setName(incomingFieldName);
											TransformField outTransTField = new TransformField(outputTransField,
													PortType.OUTPUT);

											// Set the expression to IN_<Field>
											outTransTField.setExpr(incomingTransField.getName());

											outFieldsForDummyExpr.add(outTransTField);
											logger.debug("--> add (" + outputTransField.getName() + " --> "
													+ metadataField.getName() + ") into out Fields of DummyExpr");

										} else {
											logger.error("Can't find the in identifier " + inIdentifier + " rowset");
										}
									} else {

										if (opType.equals("CONSTANT")) {

											logger.debug("Found CONSTANT operator connecting to Union. The union field will be added as transform field in dummy expression");
											TransformField outTransTField = new TransformField(incomingTransField,
													PortType.OUTPUT);

											outTransTField.setExpr(expression);

											outFieldsForDummyExpr.add(outTransTField);

											logger.debug("Add transform field " + incomingTransField.getName()
													+ " into dummy expression");
										} else {
											logger.debug("The operaor " + inOperator + " (" + opType
													+ ") is not supported.Failed to connect to transform field "
													+ inField + ". Origin expression is : " + expression);

										}

									}

									processedFields.add(incomingFieldName);

								} else {

									logger.debug(inField + " of " + inGroup + " of " + inOperator
											+ " has been added into list");
								}

							}
						}
						for (String iden : inputsTemp.keySet()) {
							PortPropagationContext linkPropContext = PortPropagationContextFactory
									.getContextForIncludeCols(inputsTemp.get(iden).getIncludeFields());
							PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(inputsTemp
									.get(iden).getExprLink());

							InputSet inSet = new InputSet(inputsTemp.get(iden).getRs(), linkPropContext, linkContext);

							allInputSetsForGroup.add(inSet);
							logger.debug("--> construct inputset for in identifier " + iden
									+ " and add into allInputSetsForGroup");
						}

						if (allInputSetsForGroup.size() > 0) {

							logger.trace("--->allInputSetsForGroup : " + allInputSetsForGroup.size() + " elements");
							logger.trace("--->outFieldsForDummyExpr : " + outFieldsForDummyExpr.size() + " elements");

							RowSet epxRS = helper.expression(allInputSetsForGroup, outFieldsForDummyExpr, exprName)
									.getRowSets().get(0);

							INFAUtils.addToExpRowSetHM(expInputSetHM, exprName, epxRS);

							String inIdentifier = exprName;
							transformQueue.offer(inIdentifier);
							logger.debug("Add the dummy expression to the transform queue of joiner");

							INFAInputSetUnit tempUnit = new INFAInputSetUnit();
							tempUnit.setRs(epxRS);
							allValidInputs.put(inIdentifier, tempUnit);

						}

					}

				} else if (upwardConnNames.size() == 0) {

					// 0:1 group (Wrong!)

					logger.error("The group (" + groupName
							+ ") doesn't have incomming connections. There are something wrong in OWB");
				}

			} else {

				// output group

				// outputGroupName = groupName;
				//
				// ArrayList<MetadataField> mFields = group.getFields();
				//
				// for (MetadataField mField : mFields) {
				//
				// ArrayList<MetadataConnection> outputConnections = mField
				// .getOutputConnections();
				//
				// if (outputConnections != null
				// && outputConnections.size() > 0) {
				// String outExpression = mField.getExpression();
				//
				// String outFieldRef = INFAUtils
				// .extractSecondPart(outExpression);
				//
				// String outFieldName = mField.getName();
				//
				// if (!outFieldName.equals(outFieldRef)) {
				// requireExpr = true;
				//
				// logger.debug("Found mismatch between the joiner output field "
				// + outFieldName
				// + " with the reference field "
				// + outFieldRef);
				// }
				//
				// outputMap.put(outFieldName, outFieldRef);
				// }
				// }
				//
				// logger.trace("Validate the output field mapping :");
				// logger.trace("\t outputField ... ReferenceField");
				// for (String outName : outputMap.keySet()) {
				// logger.trace("\t" + outName + " ... "
				// + outputMap.get(outName));
				// }
			}
		}
		logger.debug("Finish the preparation for the inputs");

		String lastJoinerName = null;

		if (transformQueue.size() > 0) {
			lastJoinerName = createJoinerFromTransformQueue(transOP, owbMapping, opName, allValidInputs, replaceClause,
					tableQueue, transformQueue);
		} else {

			if (tableQueue.size() >= 2) {

				lastJoinerName = createJoinerFromTableQueue(transOP, owbMapping, opName, allValidInputs, replaceClause,
						tableQueue);
			}
		}

		if (lastJoinerName != null) {
			logger.debug("The last joiner name is : " + lastJoinerName);

			RowSet joinRS = allValidInputs.get(lastJoinerName).getRs();
			// joinerIdentifier = opName + "." + outputGroupName;

			// if (requireExpr) {
			//
			// // An expression transform will be added after the joiner
			// ArrayList<Field> includeFields = new ArrayList<Field>();
			// Hashtable<Field, Object> exprLink = new Hashtable<Field,
			// Object>();
			//
			// for (String joinerOutputFieldName : outputMap.keySet()) {
			//
			// String refFieldName = outputMap.get(joinerOutputFieldName);
			// Field joinerReferenceField = joinRS.getField(refFieldName);
			//
			// Field expressionInputField = (Field) joinerReferenceField
			// .clone();
			//
			// expressionInputField.setName(joinerOutputFieldName);
			//
			// includeFields.add(joinerReferenceField);
			//
			// exprLink.put(joinerReferenceField, expressionInputField);
			// }
			//
			// PortPropagationContext linkPropContext =
			// PortPropagationContextFactory
			// .getContextForIncludeCols(includeFields);
			// PortLinkContext linkContext = PortLinkContextFactory
			// .getPortLinkContextByMap(exprLink);
			//
			// InputSet dummyInputSet = new InputSet(joinRS, linkPropContext,
			// linkContext);
			//
			// String exprName = INFAPropConstants
			// .getINFATransPrefix("EXPRESSION")
			// + "Naming_for_"
			// + lastJoinerName;
			//
			// RowSet expRS = helper.expression(dummyInputSet, null, exprName)
			// .getRowSets().get(0);
			//
			// INFAUtils.addToExpRowSetHM(expInputSetHM, joinerIdentifier,
			// expRS);
			// logger.debug("Add the export rowset of expression transform after the joiner "
			// + lastJoinerName
			// + " as "
			// + joinerIdentifier
			// + " into the hashmap memory");
			//
			// } else {
			//
			// INFAUtils.addToExpRowSetHM(expInputSetHM, joinerIdentifier,
			// joinRS);
			// logger.debug("Add the export rowset of Joiner transform  "
			// + joinerIdentifier + " into the hashmap memory");
			// }

			addNamingExpressionAfterJoiner(lastJoinerName, joinRS, transOP, mapping, expInputSetHM);

			logger.debug("Validate the transform " + lastJoinerName);
			Transformation joiner = mapping.getTransformation(lastJoinerName);
			INFAValidator.validateTransformInstance(joiner);

		} else {
			logger.error("The last joiner name shouldn't be null.Please check the codes");
		}

		HelperBase.printSplitLine(logger);
	}

	private String createJoinerFromTransformQueue(MetadataOperator transOP, MetadataMapping owbMapping, String opName,
			Hashtable<String, INFAInputSetUnit> allValidInputs, HashMap<String, String> replaceClause,
			Queue<String> tableQueue, Queue<String> transformQueue) throws InvalidTransformationException {
		logger.debug("Start to construct Joiners");
		HelperBase.printSplitLine(logger);

		String lastJoinerName = null;
		int joinerNO = 1;

		while (!transformQueue.isEmpty()) {
			// Because there should be at least one transform as the input for
			// the joiner, so the transform queue won't be empty

			String detailedSetName = transformQueue.poll();
			String masterSetName;

			if (detailedSetName != null) {
				masterSetName = tableQueue.poll();

				if (masterSetName != null) {

					logger.debug("Ready to construct the join with master from " + masterSetName + " and detail from "
							+ detailedSetName);

				} else {
					logger.debug("No more table , get one transfrom as input for joiner");
					if (transformQueue.size() > 0) {
						masterSetName = transformQueue.poll();
						logger.debug("Get one transform " + masterSetName + " as the input to construct the joiner");

					} else {
						logger.debug("All the inputs have been processed. Finish the joiner construction.");

						lastJoinerName = detailedSetName;
						break;
					}
				}
			} else {

				logger.error("Something wrong...");
				break;
			}

			logger.debug("Constructing the joiner ... with master (" + masterSetName + ") and detail ("
					+ detailedSetName + ")");

			String joinCondition = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, replaceClause);

			String joinerName = INFAPropConstants.getINFATransPrefix("JOINER") + INFAUtils.toCamelCase(opName)
					+ joinerNO;

			joinerNO++;

			ArrayList<InputSet> detailInputSets = new ArrayList<InputSet>();
			InputSet detailInputSet;
			InputSet masterInputSet;
			ArrayList<Field> includeFields;
			PortPropagationContext linkPropContext;
			PortLinkContext linkContext;

			RowSet detailRS = allValidInputs.get(detailedSetName).getRs();

			includeFields = allValidInputs.get(detailedSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);

				detailInputSet = new InputSet(detailRS, linkPropContext);

			} else {
				detailInputSet = new InputSet(detailRS);

			}

			logger.debug("Construct inputset for detail sorter");
			// Add sorter and parse the sortkey

			ArrayList<String> sortKeys = INFAUtils.getSortKey(detailRS, joinCondition);

			int sortKeysCnt = sortKeys.size();
			String sortPartName = detailedSetName.split("\\.")[0];
			sortPartName = INFAUtils.toCamelCase(sortPartName);
			String sortName = INFAPropConstants.getINFATransPrefix("SORT") + "Detail_for_" + joinerName;
			RowSet sortRS = null;

			if (sortKeysCnt > 0) {

				String[] sortFields = sortKeys.toArray(new String[0]);
				boolean[] sortDirections = new boolean[sortKeysCnt];

				sortRS = helper.sorter(detailInputSet, sortFields, sortDirections, sortName).getRowSets().get(0);
				logger.debug("Create sorter " + sortName);
			} else {
				logger.debug("Can't parse the condition to get the source key for detailed set " + detailedSetName
						+ ". Get random field as dummy sort key.");
				Field dummySortKey;
				if (includeFields != null && includeFields.size() > 0) {
					dummySortKey = includeFields.get(0);

				} else {
					dummySortKey = detailRS.getFields().get(0);
				}

				String sortField = dummySortKey.getName();
				boolean sortDirection = false;

				logger.debug("Get key " + sortField + " as the sort key");

				sortRS = helper.sorter(detailInputSet, sortField, sortDirection, sortName).getRowSets().get(0);
				logger.debug("Create sorter " + sortName);
			}

			if (sortRS != null) {
				// logger.debug("Create sorter for " + detailedSetName);

				allValidInputs.get(detailedSetName).setRs(sortRS);

				@SuppressWarnings("unchecked")
				Hashtable<Field, Object> sorterLink = (Hashtable<Field, Object>) allValidInputs.get(detailedSetName)
						.getExprLink().clone();

				if (sorterLink != null && sorterLink.size() > 0) {

					logger.debug("Recreate the includeFields and linkmap for detailed set");
					allValidInputs.get(detailedSetName).getIncludeFields().clear();
					allValidInputs.get(detailedSetName).getExprLink().clear();
					logger.trace("Initialize data");
					logger.trace("Find " + sorterLink.size() + " links");
					for (Field includField : sorterLink.keySet()) {

						String sortKeyName = includField.getName();

						logger.trace("Check include key " + sortKeyName);

						Field sorterField = sortRS.getField(sortKeyName);

						Field mappedField = (Field) sorterLink.get(includField);

						logger.trace("Get mapped key " + mappedField.getName());

						if (sorterField != null) {
							allValidInputs.get(detailedSetName).getIncludeFields().add(sorterField);

							allValidInputs.get(detailedSetName).getExprLink().put(sorterField, mappedField);

							logger.trace("Link sorter field " + sorterField.getName() + " to joiner field "
									+ mappedField.getName());
						} else {
							logger.debug("Can't find the sorter field ");
						}
					}
				}
				logger.debug("Replace the detail rowset in allValidInputs data structure with the key "
						+ detailedSetName);

			}

			includeFields = allValidInputs.get(detailedSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);
				linkContext = PortLinkContextFactory.getPortLinkContextByMap(allValidInputs.get(detailedSetName)
						.getExprLink());

				detailInputSet = new InputSet(allValidInputs.get(detailedSetName).getRs(), linkPropContext, linkContext);

			} else {
				detailInputSet = new InputSet(allValidInputs.get(detailedSetName).getRs());

			}

			detailInputSets.add(detailInputSet);
			logger.debug("Add the detail input sets");

			RowSet masterRS = allValidInputs.get(masterSetName).getRs();

			includeFields = allValidInputs.get(masterSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);
				masterInputSet = new InputSet(masterRS, linkPropContext);
			} else {
				masterInputSet = new InputSet(masterRS);
			}
			logger.debug("Construct inputset for master sorter");
			// Add sorter and parse the sortkey

			sortKeys = INFAUtils.getSortKey(masterRS, joinCondition);

			sortKeysCnt = sortKeys.size();
			sortPartName = masterSetName.split("\\.")[0];
			sortPartName = INFAUtils.toCamelCase(sortPartName);
			sortName = INFAPropConstants.getINFATransPrefix("SORT") + "Master_for_" + joinerName;

			sortRS = null;

			if (sortKeysCnt > 0) {

				String[] sortFields = sortKeys.toArray(new String[0]);
				boolean[] sortDirections = new boolean[sortKeysCnt];

				sortRS = helper.sorter(masterInputSet, sortFields, sortDirections, sortName).getRowSets().get(0);

				logger.debug("Create sorter " + sortName);
			} else {
				logger.debug("Can't parse the condition to get the source key for master set " + masterSetName
						+ ". Get random field as dummy sort key.");

				Field dummySortKey;
				if (includeFields != null && includeFields.size() > 0) {
					dummySortKey = includeFields.get(0);

				} else {
					dummySortKey = masterRS.getFields().get(0);
				}
				String sortField = dummySortKey.getName();
				boolean sortDirection = false;

				logger.debug("Get key " + sortField + " as the sort key");

				sortRS = helper.sorter(masterInputSet, sortField, sortDirection, sortName).getRowSets().get(0);

				logger.debug("Create sorter " + sortName);
			}

			if (sortRS != null) {
				// logger.debug("Create sorter for " + masterSetName);

				allValidInputs.get(masterSetName).setRs(sortRS);

				@SuppressWarnings("unchecked")
				Hashtable<Field, Object> sorterLink = (Hashtable<Field, Object>) allValidInputs.get(masterSetName)
						.getExprLink().clone();

				if (sorterLink != null && sorterLink.size() > 0) {

					logger.debug("Recreate the includeFields and linkmap for masterSet");
					allValidInputs.get(masterSetName).getIncludeFields().clear();
					allValidInputs.get(masterSetName).getExprLink().clear();
					logger.trace("Initialize data");
					logger.trace("Find " + sorterLink.size() + " links");

					for (Field includField : sorterLink.keySet()) {

						String sortKeyName = includField.getName();

						logger.trace("Check include key " + sortKeyName);

						Field sorterField = sortRS.getField(sortKeyName);

						Field mappedField = (Field) sorterLink.get(includField);

						logger.trace("Get mapped key " + mappedField.getName());

						if (sorterField != null) {
							includeFields.add(sorterField);

							allValidInputs.get(masterSetName).getExprLink().put(sorterField, mappedField);

							logger.trace("Link sorter field " + sorterField.getName() + " to joiner field "
									+ mappedField.getName());
						} else {
							logger.debug("Can't find the sorter field ");
						}
					}
				}

				logger.debug("Replace the rowset in allValidInputs data structure with the key " + masterSetName);

			}

			includeFields = allValidInputs.get(masterSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);
				linkContext = PortLinkContextFactory.getPortLinkContextByMap(allValidInputs.get(masterSetName)
						.getExprLink());
				masterInputSet = new InputSet(allValidInputs.get(masterSetName).getRs(), linkPropContext, linkContext);
			} else {
				masterInputSet = new InputSet(allValidInputs.get(masterSetName).getRs());
			}

			logger.debug("Add the master input sets");

			RowSet joinRowSet = (RowSet) helper.join(detailInputSets, masterInputSet, joinCondition, joinerName)
					.getRowSets().get(0);

			logger.debug("Joiner " + joinerName + " has been added into mapping");

			INFAInputSetUnit tempUnit = new INFAInputSetUnit();
			tempUnit.setRs(joinRowSet);
			allValidInputs.put(joinerName, tempUnit);

			logger.debug("The output INFAInputSetUnit set of " + joinerName + " has been added into input object list");

			transformQueue.offer(joinerName);

			logger.debug("The name of " + joinerName + " has been added into transform queue");

		}
		HelperBase.printSplitLine(logger);
		return lastJoinerName;
	}

	private String createJoinerFromTableQueue(MetadataOperator transOP, MetadataMapping owbMapping, String opName,
			Hashtable<String, INFAInputSetUnit> allValidInputs, HashMap<String, String> replaceClause,
			Queue<String> tableQueue) throws InvalidTransformationException {
		logger.debug("Start to construct Joiners");
		HelperBase.printSplitLine(logger);

		String lastJoinerName = null;
		int joinerNO = 1;

		while (!tableQueue.isEmpty()) {
			// Because there should be at least one transform as the input for
			// the joiner, so the transform queue won't be empty

			String detailedSetName = tableQueue.poll();
			String masterSetName;

			if (detailedSetName != null) {
				masterSetName = tableQueue.poll();

				if (masterSetName != null) {

					logger.debug("Ready to construct the join with master from " + masterSetName + " and detail from "
							+ detailedSetName);

				} else {
					logger.debug("No more table , get one transfrom as input for joiner");
					logger.debug("All the inputs have been processed. Finish the joiner construction.");

					lastJoinerName = detailedSetName;
					break;
				}
			} else {

				logger.error("Something wrong...");
				break;
			}

			logger.debug("Constructing the joiner ... with master (" + masterSetName + ") and detail ("
					+ detailedSetName + ")");

			String joinCondition = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, replaceClause);

			String joinerName = INFAPropConstants.getINFATransPrefix("JOINER") + INFAUtils.toCamelCase(opName)
					+ joinerNO;

			joinerNO++;

			ArrayList<InputSet> detailInputSets = new ArrayList<InputSet>();
			InputSet detailInputSet;
			InputSet masterInputSet;
			ArrayList<Field> includeFields;
			PortPropagationContext linkPropContext;
			PortLinkContext linkContext;

			RowSet detailRS = allValidInputs.get(detailedSetName).getRs();

			includeFields = allValidInputs.get(detailedSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);

				detailInputSet = new InputSet(detailRS, linkPropContext);

			} else {
				detailInputSet = new InputSet(detailRS);

			}

			logger.debug("Construct inputset for detail sorter");

			includeFields = allValidInputs.get(detailedSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);
				linkContext = PortLinkContextFactory.getPortLinkContextByMap(allValidInputs.get(detailedSetName)
						.getExprLink());

				detailInputSet = new InputSet(allValidInputs.get(detailedSetName).getRs(), linkPropContext, linkContext);

			} else {
				detailInputSet = new InputSet(allValidInputs.get(detailedSetName).getRs());

			}

			detailInputSets.add(detailInputSet);
			logger.debug("Add the detail input sets");

			includeFields = allValidInputs.get(masterSetName).getIncludeFields();
			if (includeFields != null && includeFields.size() > 0) {
				linkPropContext = PortPropagationContextFactory.getContextForIncludeCols(includeFields);
				linkContext = PortLinkContextFactory.getPortLinkContextByMap(allValidInputs.get(masterSetName)
						.getExprLink());
				masterInputSet = new InputSet(allValidInputs.get(masterSetName).getRs(), linkPropContext, linkContext);
			} else {
				masterInputSet = new InputSet(allValidInputs.get(masterSetName).getRs());
			}

			logger.debug("Add the master input sets");

			RowSet joinRowSet = (RowSet) helper.join(detailInputSets, masterInputSet, joinCondition, joinerName)
					.getRowSets().get(0);

			logger.debug("Joiner " + joinerName + " has been added into mapping");

			INFAInputSetUnit tempUnit = new INFAInputSetUnit();
			tempUnit.setRs(joinRowSet);
			allValidInputs.put(joinerName, tempUnit);

			logger.debug("The output INFAInputSetUnit set of " + joinerName + " has been added into input object list");

			tableQueue.offer(joinerName);

			logger.debug("The name of " + joinerName + " has been added into transform queue");

		}
		HelperBase.printSplitLine(logger);
		return lastJoinerName;
	}

	private void addNamingExpressionAfterJoiner(String joinerName, RowSet joinRS, MetadataOperator transOP,
			Mapping mapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException {

		HelperBase.printSplitLine(logger);
		logger.debug("Add naming expresson transform for joiner " + joinerName);
		HelperBase.printSplitLine(logger);

		logger.debug("Check whether the output fields needing naming conversion");
		boolean requireExpr = false;

		String outputGroupName = transOP.getOutputGroupNames().get(0);
		String joinerIdentifier = transOP.getName() + "." + outputGroupName;

		// The hashmap should contain the output field at first place and the
		// reference field in the second place.
		HashMap<String, String> outputMap = new HashMap<String, String>();

		MetadataGroup outGroup = transOP.getGroupByName(outputGroupName);

		if (outGroup != null) {
			// output group

			ArrayList<MetadataField> mFields = outGroup.getFields();

			for (MetadataField mField : mFields) {

				ArrayList<MetadataConnection> outputConnections = mField.getOutputConnections();

				if (outputConnections != null && outputConnections.size() > 0) {
					String outExpression = mField.getExpression();

					String outFieldRef = INFAUtils.extractSecondPart(outExpression);

					String outFieldName = mField.getName();

					if (!outFieldName.equals(outFieldRef)) {
						requireExpr = true;

						logger.debug("Found mismatch between the joiner output field " + outFieldName
								+ " with the reference field " + outFieldRef);
					}

					outputMap.put(outFieldName, outFieldRef);
				}
			}

			logger.trace("Validate the output field mapping :");
			logger.trace("\t outputField ... ReferenceField");
			for (String outName : outputMap.keySet()) {
				logger.trace("\t" + outName + " ... " + outputMap.get(outName));
			}
		}

		if (requireExpr) {

			logger.debug("An expression transform will be added after the joiner for naming conversion");
			ArrayList<Field> includeFields = new ArrayList<Field>();
			Hashtable<Field, Object> exprLink = new Hashtable<Field, Object>();

			ArrayList<TransformField> outputFields = new ArrayList<TransformField>();

			String inputPrefix = "IN_";

			logger.debug("Construct the input set for the naming expression");
			for (String refFieldName : outputMap.values()) {

				Field joinerReferenceField = joinRS.getField(refFieldName);

				if (joinerReferenceField == null) {
					logger.debug("Can't find field " + refFieldName + " in the joiner output rowset. Take another try.");

					joinerReferenceField = INFAUtils.tryOtherPossibleName(joinRS, refFieldName);

				}

				Field expressionInputField = (Field) joinerReferenceField.clone();

				expressionInputField.setName(inputPrefix + refFieldName);

				includeFields.add(joinerReferenceField);

				exprLink.put(joinerReferenceField, expressionInputField);

				logger.trace(refFieldName + "  -->  " + expressionInputField.getName());
			}

			logger.debug("Construct the output set for the naming expression");

			for (String joinerOutputFieldName : outputMap.keySet()) {

				String refFieldName = outputMap.get(joinerOutputFieldName);

				Field joinerReferenceField = joinRS.getField(refFieldName);

				if (joinerReferenceField == null) {
					logger.debug("Can't find field " + refFieldName + " in the joiner output rowset. Take another try.");

					joinerReferenceField = INFAUtils.tryOtherPossibleName(joinRS, refFieldName);

				}

				Field expressionOutputField = (Field) joinerReferenceField.clone();

				expressionOutputField.setName(joinerOutputFieldName);

				TransformField outputField = new TransformField(expressionOutputField, PortType.OUTPUT);

				outputField.setExpr(inputPrefix + refFieldName);

				outputFields.add(outputField);

				logger.trace(" + " + joinerOutputFieldName);
			}

			PortPropagationContext linkPropContext = PortPropagationContextFactory
					.getContextForIncludeCols(includeFields);
			PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(exprLink);

			InputSet dummyInputSet = new InputSet(joinRS, linkPropContext, linkContext);

			ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

			allInputSets.add(dummyInputSet);

			String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + "Naming_for_" + joinerName;

			String exprDesc = "This expression is used to connect the source fields of joiner to the output fields because they have difference name";

			// TransformHelper helper = new TransformHelper(mapping);

			RowSet expRS = helper.expression(allInputSets, outputFields, exprName).getRowSets().get(0);

			Transformation exp = mapping.getTransformation(exprName);

			exp.setDescription(exprDesc);

			INFAUtils.addToExpRowSetHM(expInputSetHM, joinerIdentifier, expRS);
			logger.debug("Add the export rowset of expression transform after the joiner DSQ " + joinerName + " as "
					+ joinerIdentifier + " into the hashmap memory");

		} else {

			INFAUtils.addToExpRowSetHM(expInputSetHM, joinerIdentifier, joinRS);
			logger.debug("Add the export rowset of Joiner DSQ transform  " + joinerIdentifier
					+ " into the hashmap memory");
		}
	}

	@SuppressWarnings("unused")
	public void addFilterTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Filter transform " + transOP.getName());
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();
		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}
		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for filter " + transName
				+ " have been processed and added into export set");

		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

		// replaceClause is to replace the constant expression in filter
		// expression

		logger.trace("The memory hashmap has following entries for FILTER transform:");
		for (String key : expInputSetHM.keySet()) {
			logger.trace(key);
		}

		HashMap<String, String> replacedClause = new HashMap<String, String>();

		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter;

		inIdentifierIter = incomingRelatedOPs.iterator();

		boolean needExprTrans = false;

		while (inIdentifierIter.hasNext()) {

			String inIdentifier = inIdentifierIter.next();

			logger.debug("Start to contruct the input set for incoming identifier " + inIdentifier);

			RowSet expRS = null;

			logger.debug("Get export row set for identifier " + inIdentifier);

			ArrayList<Field> includeFields = new ArrayList<Field>();
			java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

			ArrayList<MetadataGroup> groups = transOP.getGroups();
			for (MetadataGroup group : groups) {

				String direction = group.getDirection();
				if (direction.contains("IN")) {
					logger.debug("Loop the every incoming field for incoming group " + group.getName());

					ArrayList<MetadataField> mFields = group.getFields();

					String URI = null;
					for (MetadataField mField : mFields) {
						logger.trace("processing field " + mField.getName());

						ArrayList<MetadataConnection> inConns = mField.getInputConnections();
						if (inConns != null && inConns.size() > 0) {
							MetadataConnection inConn = inConns.get(0);
							String inOPName = inConn.getSourceOperator();
							String inGroup = inConn.getSourceGroup();
							String inField = inConn.getSourceField();

							MetadataOperator mOP = owbMapping.getOperatorByName(inOPName);
							// String bizName = mOP.getBusiness_name();

							if (inIdentifier.equals(inOPName + "." + inGroup)) {

								logger.debug("The field " + mField.getName()
										+ " has incoming connection from identifier : " + inIdentifier);

								HashSet<String> incomings = mOP.getIncomingRelationships();

								String opType = mOP.getType();

								if (OWBPropConstants.isSupportedOWBSourceOperator(opType)
										|| (incomings != null && incomings.size() > 0)) {

									if (incomings != null && incomings.size() > 0) {

										// Besides of table and isolated
										// operator, if there are more
										// operators connecting to FILTER, we
										// need create an expression
										// before it

										needExprTrans = true;

										logger.debug("According to the inputs of FILTER, an expression transform will be created before FITLER "
												+ transName);
									}

									if (expRS == null) {

										// Initial the rowset
										if (mOP.isSource()) {
											String srcIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE")
													+ inOPName;

											expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, srcIdentifier);

											URI = srcIdentifier;
											logger.debug("Get export row set for source " + srcIdentifier);

											List<Field> rsFields = expRS.getFields();
											for (Field rsField : rsFields) {
												logger.trace("\t ... " + rsField.getName());
											}

										} else {
											expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);

											URI = inIdentifier;
											logger.debug("Get export row set for identifier " + inIdentifier);

											List<Field> rsFields = expRS.getFields();
											for (Field rsField : rsFields) {
												logger.trace("\t ... " + rsField.getName());
											}
										}
									}

									if (expRS != null) {

										Field iField = expRS.getField(inField);

										Field tField = INFAUtils.convertToField(mField, FieldType.TRANSFORM);

										if (iField != null && tField != null) {
											link.put(iField, tField);
											includeFields.add(iField);

											logger.debug("Link upward field " + iField.getName() + " to target field "
													+ tField.getName());

										} else if (iField == null) {
											logger.debug("Can't find " + inField + " in the upward Rowset of " + URI);
										} else if (tField == null) {
											logger.debug("The field " + mField + " of " + group.getName()
													+ " in Joiner is failed to constructed");
										}
									} else {
										logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
									}
								} else {
									logger.debug("The connected operator " + inOPName + " (" + opType
											+ ") will be ommitted for Filter operator or convert to expression");

									MetadataField replacedField = mOP.getGroupByName(inGroup).getFieldByName(inOPName);

									if (replacedField != null) {
										String replacedExpression = replacedField.getExpression();

										replacedClause.put(mField.getName(), replacedExpression);
									} else {
										logger.error("The filed " + inField + " can't be found in " + inOPName + "."
												+ inGroup);
									}
								}
							}
						} else {
							logger.debug("The field " + mField.getName() + " is not linked.Skip it.");
						}

					}

				} else {
					logger.debug("The group is not IN group. Skip it");
				}
			}

			if (expRS != null) {

				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(includeFields);
				PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(link);

				InputSet inSet = new InputSet(expRS, linkPropContext, linkContext);

				allInputSets.add(inSet);
			} else {
				logger.error("Can't find the rowset " + inIdentifier);
			}
		}

		String filterExpression = null;

		if (replacedClause.size() > 0) {

			logger.debug("Found replaced clause");

			filterExpression = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, replacedClause);

		} else {
			logger.debug("No need to replace clause in the filter condition ");

			filterExpression = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, null);

		}

		// TransformHelper helper = new TransformHelper(mapping);
		String filterName = INFAPropConstants.getINFATransPrefix("FILTER") + transName;
		OutputSet output = null;
		RowSet filtRS = null;
		if (needExprTrans) {

			String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + "Before_" + transName;

			List<TransformField> dummyTansforms = new ArrayList<TransformField>();

			RowSet expRS = helper.expression(allInputSets, dummyTansforms, exprName).getRowSets().get(0);
			logger.debug("Add expression " + exprName);

			output = helper.filter(expRS, filterExpression, filterName);

		} else {

			output = helper.filter(allInputSets, filterExpression, filterName);
		}

		logger.debug("Add filter transform " + filterName);

		filtRS = output.getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, filtRS);

		logger.debug("Filter " + transName + "(" + identifier
				+ ") has been added as a FILTER transform and into memory hashmap.");

		logger.debug("Validate the filter transform " + filterName);

		Transformation filter = mapping.getTransformation(filterName);

		INFAValidator.validateTransformInstance(filter);

		HelperBase.printSplitLine(logger);

	}

	public void addLookupTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		String transName = transOP.getName();
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Lookup transform " + transName);
		HelperBase.printSplitLine(logger);

		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for lookup " + transName
				+ " have been processed and added into export set");

		String incomingPrefix = "IN_";
		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();
		HashMap<String, String> replacedClause = new HashMap<String, String>();

		ArrayList<String> outputFieldNames = new ArrayList<String>();

		// Get input fields

		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter;
		inIdentifierIter = incomingRelatedOPs.iterator();

		while (inIdentifierIter.hasNext()) {

			String inIdentifier = inIdentifierIter.next();

			logger.debug("Start to contruct the input set for incoming identifier " + inIdentifier);

			RowSet expRS = null;

			ArrayList<Field> includeFields = new ArrayList<Field>();
			java.util.Hashtable<Field, Object> link = new java.util.Hashtable<Field, Object>();

			ArrayList<MetadataGroup> groups = transOP.getGroups();
			for (MetadataGroup group : groups) {

				String direction = group.getDirection();
				if (direction.contains("IN")) {

					// Replace the group name
					replacedClause.put(group.getName() + ".", incomingPrefix);
					logger.debug("Replace the in group name with the prefix " + incomingPrefix);

					logger.debug("Loop the every incoming field for incoming group " + group.getName());

					ArrayList<MetadataField> mFields = group.getFields();

					for (MetadataField mField : mFields) {

						ArrayList<MetadataConnection> inConns = mField.getInputConnections();
						String name = mField.getName();
						if (inConns != null) {
							MetadataConnection inConn = inConns.get(0);
							String inOPName = inConn.getSourceOperator();
							String inGroup = inConn.getSourceGroup();
							String inField = inConn.getSourceField();

							MetadataOperator mOP = owbMapping.getOperatorByName(inOPName);
							MetadataGroup mGp = mOP.getGroupByName(inGroup);
							MetadataField mFd = mGp.getFieldByName(inField);

							String opType = mOP.getType();

							if (INFAUtils.isSupportedOWBTransform(opType)
									&& inIdentifier.equals(inOPName + "." + inGroup)) {

								logger.debug("The field " + name + " has incoming connection from identifier : "
										+ inIdentifier);

								if (expRS == null) {

									// Initial the rowset
									if (mOP.isSource()) {
										String srcIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE")
												+ inOPName;

										expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, srcIdentifier);

										logger.debug("Get export row set for source " + srcIdentifier);
									} else {
										expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);

										logger.debug("Get export row set for identifier " + inIdentifier);

									}
								}

								if (expRS != null) {

									Field iField = expRS.getField(inField);

									// Use the same sourcing field
									Field tField = INFAUtils.convertToField(mFd, FieldType.TRANSFORM);

									// Rename incoming fields
									tField.setName(incomingPrefix + name);

									if (iField != null && tField != null) {
										link.put(iField, tField);
										includeFields.add(iField);

										logger.debug("Link upward field " + iField.getName() + " to target field "
												+ tField.getName());

									}
								} else {
									logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
								}
							}

						} else {
							logger.debug("The field " + name + " is not linked.Skip it.");
						}

					}

				} else {
					logger.debug("The group is OUT group.");

					ArrayList<MetadataField> mFields = group.getFields();

					for (MetadataField mField : mFields) {

						String name = mField.getName();

						ArrayList<MetadataConnection> outConns = mField.getOutputConnections();
						if (outConns != null && outConns.size() > 0) {
							// Get output field name

							outputFieldNames.add(name);

							logger.debug("Find output field " + name);
						}

					}
					// Replace the group name

					replacedClause.put(group.getName() + ".", "");
					logger.debug("Replace the out group with empty string");
				}
			}
			if (expRS != null) {
				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(includeFields);
				PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(link);

				InputSet inSet = new InputSet(expRS, linkPropContext, linkContext);

				allInputSets.add(inSet);
			} else {
				logger.error("Can't find the rowset " + inIdentifier);
			}

		}

		// Get the rewritten lookup condition
		replacedClause.put("(+)", "");

		String lkpCondition = SQLProcessor.fetchAndFormatCondition(owbMapping, transOP, replacedClause);

		logger.debug("The lookup condition is : " + lkpCondition);

		// Get lookup table

		SourceTarget lkpTable = null;

		ShortCut scLkpTable = null;

		String lkpTableName = SQLProcessor.getOperatorProperty(transOP, "LOOKUP_OBJECT_NAME");

		String infaLoc = INFAPropConstants.getINFALocation("SOURCE", "*");
		logger.debug("Get default informatica dbd " + infaLoc);

		if (lkpTableName != null) {

			scLkpTable = addIfLookupTableMissing(folder, rep, lkpTableName);

			if (scLkpTable != null) {
				// Construct lookup transform

				String lkpTransName = INFAPropConstants.getINFATransPrefix("LOOKUP") + transName;

				// TransformHelper helper = new TransformHelper(mapping);

				lkpTable = (SourceTarget) scLkpTable.getRefObject();

				OutputSet lkpOutputSet = helper.lookup(allInputSets, lkpTable, lkpCondition, lkpTransName);

				RowSet lookupRS = (RowSet) lkpOutputSet.getRowSets().get(0);

				logger.debug("Add LOOKUP transform " + lkpTransName);

				// Identify if exists no match row (outer join)

				String hasNoMatchRow = SQLProcessor.getOperatorProperty(transOP, "CREATE_NO_MATCH_ROW");

				int outFieldCnt = outputFieldNames.size();

				if (hasNoMatchRow != null && hasNoMatchRow.equalsIgnoreCase("true") && outFieldCnt > 0) {

					boolean requireExp = false;

					logger.debug("The lookup has left outer join. So an expression transform is needed for default values ");

					// Create expression for the out field with default value

					ArrayList<InputSet> expInputs = new ArrayList<InputSet>();
					ArrayList<Field> lkpOutFields = new ArrayList<Field>();
					ArrayList<TransformField> exprOutFields = new ArrayList<TransformField>();
					Hashtable<Field, Object> link = new Hashtable<Field, Object>();

					for (String fieldName : outputFieldNames) {
						Field field = lookupRS.getField(fieldName);

						if (field != null) {
							logger.debug("Porcess the field " + fieldName);

							lkpOutFields.add(field);
							Field expField = (Field) field.clone();

							expField.setName(incomingPrefix + fieldName);
							link.put(field, expField);
							logger.debug("Link source field " + fieldName + " to expression incoming field "
									+ expField.getName());

							String defaultValue = INFAUtils.getTypeDefaultValue(field.getDataType());

							if (defaultValue.replace("'", "").trim().length() != 0) {
								requireExp = true;
								logger.debug("Found valid default value (the length is " + defaultValue.trim().length()
										+ ")");
							}
							TransformField expOutField = new TransformField(field, PortType.OUTPUT);
							expOutField.setExpr("IIF(ISNULL(" + expField.getName() + ")," + defaultValue + ","
									+ expField.getName() + ")");
							exprOutFields.add(expOutField);

							logger.debug("Add output transform field " + fieldName);

						} else {
							logger.error("The filed " + fieldName + " is not existed in export rowset from lookup "
									+ lkpTransName);
						}
					}

					if (requireExp) {
						PortPropagationContext linkPropContext = PortPropagationContextFactory
								.getContextForIncludeCols(lkpOutFields);
						PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(link);

						InputSet expInput = new InputSet(lookupRS, linkPropContext, linkContext);
						// InputSet expInput = new InputSet(lookupRS,
						// linkPropContext);
						expInputs.add(expInput);

						String exprTransName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + transName
								+ "_Defaults";

						OutputSet exprOutputSet = helper.expression(expInputs, exprOutFields, exprTransName);

						RowSet exprRS = (RowSet) exprOutputSet.getRowSets().get(0);
						logger.debug("The expression " + exprTransName + " has been added into mapping with "
								+ exprOutFields.size() + " output fields");

						// Store expression transform output rowset to hashmap
						// memory

						INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, exprRS);

						logger.debug("Lookup operator " + transName + "(" + identifier
								+ ") has been added as a LOOKUP + Expression transform "
								+ "and the rowset of expression transform has been added into memory hashmap.");

					} else {
						logger.debug("The default values are empty string. So we don't additonal expression after lookup");

						INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, lookupRS);

						logger.debug("Lookup operator " + transName + "(" + identifier
								+ ") has been added as a LOOKUP transform and into memory hashmap.");

					}

				} else {
					// Store lookup transform rowset to hashmap memory

					INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, lookupRS);

					logger.debug("Lookup operator " + transName + "(" + identifier
							+ ") has been added as a LOOKUP transform and into memory hashmap.");
				}
			} else {

				logger.error("Can't find the lookup shortcut or failed to create shortcut for the lookup table in local folder");
			}
		} else {
			logger.error("The property LOOKUP_OBJECT_NAME is not defined for the lookup operator " + transName);
		}

		HelperBase.printSplitLine(logger);
	}

	private ShortCut addIfLookupTableMissing(Folder folder, Repository rep, String lkpTableName)
			throws RepoOperationException, MapFwkReaderException {
		ShortCut scLkpTable;
		logger.debug("Ready to get lookup table " + lkpTableName);
		// lkpTable = INFAUtils.getAnySourceTarget(rep, lkpTableName, infaLoc);

		scLkpTable = INFAUtils.getShortCutFromFolder(folder, lkpTableName, RepositoryObjectConstants.OBJTYPE_TARGET);

		if (scLkpTable == null) {
			logger.debug("Failed to find the lookup table " + lkpTableName
					+ " under local folder target. Continue to try source under local folder");
			scLkpTable = INFAUtils
					.getShortCutFromFolder(folder, lkpTableName, RepositoryObjectConstants.OBJTYPE_SOURCE);

			if (scLkpTable == null) {

				logger.debug("Add the shortcut of lookup table into local folder");

				logger.debug("First try to add the shortcut from target from SHARED folder");

				Target lookupTgt = INFATarget.getTargetFromShareFolder(rep, lkpTableName);

				if (lookupTgt != null) {
					scLkpTable = INFATarget.createShortCutFromTarget(lookupTgt, rep, folder);
					if (scLkpTable != null) {

						logger.debug("Create Shortcut " + scLkpTable.getName() + " for target " + lkpTableName
								+ " on folder " + scLkpTable.getFolderName());

						folder.addShortCut(scLkpTable);
						folder.addShortCutInternal(scLkpTable);

					} else {
						logger.error("Can't create the shortcut for table " + lkpTableName
								+ " from shared folder target");
					}

					logger.debug("Add lookup shortcut into local folder");
				} else {
					logger.debug("Failed to find the table in shared target folder. Then try to add the shortcut from source from SHARED folder");

					Source lookupSource = INFASource.getSourceFromShareFolder(rep, lkpTableName, "*");

					if (lookupSource != null) {
						scLkpTable = INFASource.createShortCutFromSource(lookupSource, rep, folder);

						if (scLkpTable != null) {
							logger.debug("Create Shortcut " + scLkpTable.getName() + " for source " + lkpTableName
									+ " on folder " + scLkpTable.getFolderName());
							folder.addShortCut(scLkpTable);
							folder.addShortCutInternal(scLkpTable); // for fetch
						} else {
							logger.error("Can't create the shortcut for table " + lkpTableName
									+ " from shared folder source");
						}

					}
				}

			} else {
				logger.debug("Get the lookup table : " + scLkpTable.getRefrenceObjName() + " from local folder target");
			}
		} else {

			logger.debug("Get the lookup table : " + scLkpTable.getRefrenceObjName() + " from local folder target");
		}
		return scLkpTable;
	}

	public void addExpressTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Expression transform");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();

		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for Expression " + transName
				+ " have been processed and added into export set");

		List<TransformField> transformFields = new ArrayList<TransformField>();
		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();
		ArrayList<Field> includeFields = new ArrayList<Field>();
		Hashtable<Field, Object> exprLink = new Hashtable<Field, Object>();

		// Get input fields

		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter;
		inIdentifierIter = incomingRelatedOPs.iterator();

		while (inIdentifierIter.hasNext()) {

			String inIdentifier = inIdentifierIter.next();

			logger.debug("Start to contruct the input set for incoming identifier " + inIdentifier);

			RowSet expRS = null;

			logger.debug("Get export row set for identifier " + inIdentifier);

			includeFields.clear();
			exprLink.clear();

			ArrayList<MetadataGroup> incomingGroups = transOP.getGroups();

			HashMap<String, String> outFieldsMap = new HashMap<String, String>();

			for (MetadataGroup inComingGroup : incomingGroups) {

				if (inComingGroup.getDirection().matches(".*IN.*")) {

					outFieldsMap.put(inComingGroup.getName() + ".", "");

					List<MetadataField> allIncomingFields = inComingGroup.getFields();

					HashSet<String> processedFields = new HashSet<String>();

					for (MetadataField metadataField : allIncomingFields) {
						String incomingFieldName = metadataField.getName();

						logger.debug("Processing the field " + incomingFieldName + " in this expression transform");
						ArrayList<MetadataConnection> inputConnections = metadataField.getInputConnections();

						if (inputConnections != null && inputConnections.size() > 0) {
							MetadataConnection inConnection = inputConnections.get(0);

							String inField = inConnection.getSourceField();
							String inGroup = inConnection.getSourceGroup();
							String inOperator = inConnection.getSourceOperator();

							logger.debug("Processing the incoming " + inOperator + "." + inGroup + "." + inField);

							MetadataOperator mOP = owbMapping.getOperatorByName(inOperator);
							MetadataGroup mGp = mOP.getGroupByName(inGroup);
							MetadataField mFd = mGp.getFieldByName(inField);

							// String bizName = mOP.getBusiness_name();

							if (inIdentifier.equals(inOperator + "." + inGroup)) {

								logger.debug("The field " + incomingFieldName
										+ " matches the incoming connection from identifier : " + inIdentifier);

								String expression = mFd.getExpression();
								expression = expression.replace("INGRP1.", "");
								expression = expression + " \n-- need manual fix";

								String opType = mOP.getType();

								if (!processedFields.contains(incomingFieldName)) {
									logger.debug("Ready to add the expression field " + incomingFieldName);

									Field incomingTransField = INFAUtils.convertToField(metadataField,
											FieldType.TRANSFORM);

									if (INFAUtils.isSupportedOWBTransform(opType)) {

										logger.debug("The incomming connection is from supported operator (type : "
												+ opType + ")");

										if (expRS == null) {

											// Initial the rowset
											if (mOP.isSource()) {
												String srcIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE")
														+ inOperator;

												expRS = INFAUtils
														.getExpRowSet(owbMapping, expInputSetHM, srcIdentifier);

												logger.debug("Get export row set for source " + inOperator);
											} else {
												expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);

												logger.debug("Get export row set for identifier " + inIdentifier);

											}
										}

										if (expRS != null) {
											// TransformField tField1 = new
											// TransformField(targetField,
											// PortType.INPUT_OUTPUT);

											// transformFields.add(tField1);
											logger.debug("Add " + inField + " of " + inGroup + " of " + inOperator
													+ " into transformField list");

											Field sourcingField = expRS.getField(inField);

											if (sourcingField != null) {

												incomingTransField.setName("IN_" + incomingFieldName);

												String newName = incomingTransField.getName();
												logger.debug("-- In Field of Expression transform (" + newName
														+ ") is connected from " + sourcingField.getName() + " of "
														+ inGroup + " of " + inOperator);

												outFieldsMap.put(incomingFieldName, newName);

												includeFields.add(sourcingField);
												exprLink.put(sourcingField, incomingTransField);

												logger.debug("Transform Field " + newName + " is connected from "
														+ sourcingField.getName() + " of " + inGroup + " of "
														+ inOperator);
											} else {
												logger.error("Can't find the incoming field " + inField
														+ " from upward rowset");
											}

										} else {
											logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
										}

									} else if (opType.equals("CONSTANT")) {

										logger.debug("The incomming connection is from CONSTANT field");

										TransformField tField = new TransformField(incomingTransField, PortType.OUTPUT);

										logger.debug("Add " + inField + " of " + inGroup + " of " + inOperator
												+ " into transformField list");
										if (expression.contains(".")) {
											logger.error("The expression for transform field " + incomingFieldName
													+ " may contain database related expression.");
											tField.setExpr("/* The expression for target field "
													+ incomingFieldName
													+ " may contain database related expression. Please create SQL transform for this field. The expression is : "
													+ expression + " */");
										} else {
											tField.setExpr(expression);
										}

										transformFields.add(tField);

									} else if (opType.equals("SEQUENCE")) {

										String exprString = "double(15,0) " + incomingFieldName + "=:SP.SEQ_NEXTVAL('"
												+ expression + "')";

										TransformField seqField = new TransformField(exprString);

										transformFields.add(seqField);
									} else {

										logger.error("The operaor " + inOperator + " (" + opType
												+ ") is not supported.Failed to connect to transform field " + inField);

										TransformField tField = new TransformField(incomingTransField, PortType.OUTPUT);

										tField.setExpr("/* This field is from unsupported OWB operator. Origin expression is : "
												+ expression + " */");
										transformFields.add(tField);

										logger.debug("For compatible,  only create dummy target filed");

									}

									processedFields.add(incomingFieldName);

								} else {

									logger.debug(inField + " of " + inGroup + " of " + inOperator
											+ " has been added into transformField list");
								}
							} else {

								logger.debug("Bypass the field " + incomingFieldName
										+ " becasue it doesn't match the incoming identifier (" + inIdentifier
										+ ") we are looking for ");
							}
						}
					}

					if (expRS != null) {

						PortPropagationContext linkPropContext = PortPropagationContextFactory
								.getContextForIncludeCols(includeFields);
						PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(exprLink);

						InputSet inSet = new InputSet(expRS, linkPropContext, linkContext);

						allInputSets.add(inSet);
					} else {
						logger.error("Can't find the rowset " + inIdentifier);
					}

				} else {
					logger.debug("Process the out group for EXPRESSION transform " + transName);

					List<MetadataField> allOutFields = inComingGroup.getFields();

					for (MetadataField metadataField : allOutFields) {

						Field incomingTransField = INFAUtils.convertToField(metadataField, FieldType.TRANSFORM);

						TransformField tField = new TransformField(incomingTransField, PortType.OUTPUT);

						String expression = metadataField.getExpression();
						expression = SQLProcessor.replaceClause(expression, outFieldsMap);

						String exprType = metadataField.getData_type();
						tField.setExpr(expression);
						tField.setExpressionType(exprType);

						transformFields.add(tField);

						logger.debug("EXPRESSION : Add output field (" + metadataField.getName()
								+ ") with expression (" + expression + ")");

					}
				}
			}

		}

		logger.debug("Finish the transform field processing");

		String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + transName;
		;

		ExpTransformation exprTrans = new ExpTransformation(exprName, transName,
				"Expression Transformation for OWB operator ", exprName, mapping, allInputSets, transformFields, null);

		logger.debug("Add Expression transform  " + exprName + " into the mapping");

		RowSet exprRS = exprTrans.apply().getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, exprRS);
		logger.debug("Add the export rowset of Dummy Expression transform  " + exprName + " into the hashmap memory");

		HelperBase.printSplitLine(logger);

	}

	public void addUnionTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Union transform");
		HelperBase.printSplitLine(logger);

		// TransformHelper helper = new TransformHelper(mapping);

		String transName = transOP.getName();

		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for Union " + transName
				+ " have been processed and added into export set");

		HelperBase.printSplitLine(logger);

		// Sort the union ports
		logger.debug("Ready to process the union field data structure");

		TreeSet<String> outputFieldNames = new TreeSet<String>();
		Hashtable<String, ArrayList<String>> unionFieldMap = new Hashtable<String, ArrayList<String>>();
		Queue<String> outputQueue = new LinkedList<String>();

		MetadataGroup unionOutputGrp = null;

		ArrayList<MetadataGroup> groups = transOP.getGroups();

		for (MetadataGroup group : groups) {

			if (group.getDirection().equals("OUTGRP")) {

				unionOutputGrp = group;

				List<MetadataField> allOutputFields = group.getFields();
				for (MetadataField metadataField : allOutputFields) {
					String OutputFieldName = metadataField.getName();

					outputFieldNames.add(OutputFieldName);

					outputQueue.offer(OutputFieldName);

					logger.debug("Add output field (" + OutputFieldName + ") of Union into queue");
				}
			}
		}

		logger.debug("The output queue looks like : " + outputQueue);

		for (MetadataGroup group : groups) {

			Iterator<String> outputItr = outputQueue.iterator();

			if (group.getDirection().matches(".*IN.*")) {
				List<MetadataField> allIncomingFields = group.getFields();
				for (MetadataField metadataField : allIncomingFields) {
					String incomingFieldName = metadataField.getName();

					if (outputItr.hasNext()) {
						String outputFieldName = outputItr.next();

						if (unionFieldMap.get(outputFieldName) == null) {
							ArrayList<String> inFields = new ArrayList<String>();

							inFields.add(incomingFieldName);
							unionFieldMap.put(outputFieldName, inFields);
						} else {
							unionFieldMap.get(outputFieldName).add(incomingFieldName);

						}
						logger.debug("Map the output field (" + outputFieldName + ") of Union to incomming field ("
								+ incomingFieldName + ")");
					}
				}
			}
		}

		logger.debug("** To process the UNION fields order as : " + outputFieldNames.toString());

		logger.debug("Create dummy expressions before construct the UNION transform");

		ArrayList<MetadataGroup> allGroups = groups;

		for (MetadataGroup inComingGroup : allGroups) {
			logger.debug("Processing the group of union : " + inComingGroup.getName());
			if (inComingGroup.getDirection().matches(".*IN.*")) {

				// Construnct the dummy Expression
				String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION")
						+ INFAPropConstants.getINFATransPrefix("UNION") + inComingGroup.getName();

				// ExpTransformation dummyExpr = new ExpTransformation(exprName,
				// exprName,
				// "Dummy expression Union transform " + transName, exprName);

				ArrayList<InputSet> allInputSetsForGroup = new ArrayList<InputSet>();
				ArrayList<TransformField> outFieldsForDummyExpr = new ArrayList<TransformField>();

				HashSet<String> processedFields = new HashSet<String>();

				Hashtable<String, INFAInputSetUnit> inputsTemp = new Hashtable<String, INFAInputSetUnit>();

				Iterator<String> itIncludeFieldNames = outputFieldNames.iterator();

				while (itIncludeFieldNames.hasNext()) {
					String incomingFieldName = itIncludeFieldNames.next();

					MetadataField metadataField = inComingGroup.getFieldByName(incomingFieldName);

					if (metadataField == null) {

						Iterator<String> mappedFields = unionFieldMap.get(incomingFieldName).iterator();

						while (mappedFields.hasNext()) {
							String tempName = mappedFields.next();
							if (!tempName.equals(incomingFieldName)) {
								logger.debug("The output field has different name (" + incomingFieldName
										+ ") with field " + tempName + " in group " + inComingGroup.getName());

								// incomingFieldName = tempName;
								metadataField = inComingGroup.getFieldByName(tempName);

								if (metadataField != null) {
									break;
								}
							} else {
								continue;
							}
						}
					}
					logger.debug("Processing the field " + incomingFieldName + " in this transform");
					ArrayList<MetadataConnection> inputConnections = metadataField.getInputConnections();

					if (inputConnections != null && inputConnections.size() > 0) {
						MetadataConnection inConnection = inputConnections.get(0);

						String inField = inConnection.getSourceField();
						String inGroup = inConnection.getSourceGroup();
						String inOperator = inConnection.getSourceOperator();

						logger.debug("Processing the incoming " + inOperator + "." + inGroup + "." + inField);

						MetadataOperator mOP = owbMapping.getOperatorByName(inOperator);
						MetadataGroup mGp = mOP.getGroupByName(inGroup);
						MetadataField mFd = mGp.getFieldByName(inField);

						String inIdentifier;
						if (mOP.isSource()) {
							inIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE") + inOperator;
						} else {
							inIdentifier = inOperator + "." + inGroup;
						}

						String expression = mFd.getExpression();
						String opType = mOP.getType();

						if (!processedFields.contains(incomingFieldName)) {
							logger.debug("Ready to add the field " + incomingFieldName);

							Field incomingTransField = INFAUtils.convertToField(metadataField, FieldType.TRANSFORM);

							if (INFAUtils.isSupportedOWBTransform(opType)) {
								logger.debug("The incomming connection is from supported operator (type : " + opType
										+ ")");

								if (inputsTemp.get(inIdentifier) == null) {

									logger.debug("Initial InputSet unit instance for in indentifier " + inIdentifier);

									INFAInputSetUnit tempUnit = new INFAInputSetUnit();

									RowSet expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);

									tempUnit.setRs(expRS);

									inputsTemp.put(inIdentifier, tempUnit);

									logger.debug("Create inputsTemp for in identifier " + inIdentifier);

								}
								if (inputsTemp.get(inIdentifier).getRs() != null) {

									logger.debug("Get export row set for identifier " + inIdentifier);

									logger.debug("Add " + inField + " of " + inGroup + " of " + inOperator
											+ " into transformField list");

									Field sourcingField = inputsTemp.get(inIdentifier).getRs().getField(inField);
									inputsTemp.get(inIdentifier).getIncludeFields().add(sourcingField);

									incomingTransField.setName("IN_" + incomingTransField.getName());

									inputsTemp.get(inIdentifier).getExprLink().put(sourcingField, incomingTransField);

									logger.debug("-- In Field of Expression transform (" + incomingTransField.getName()
											+ ") is connected from " + sourcingField.getName() + " of " + inGroup
											+ " of " + inOperator);

									/*
									 * The field should be fetched from union
									 * output group, because sometimes the input
									 * group may have different name or data
									 * type of fields.
									 */

									metadataField = unionOutputGrp.getFieldByName(incomingFieldName);
									Field outputTransField = INFAUtils.convertToField(metadataField,
											FieldType.TRANSFORM);

									// Field outputTransField = (Field)
									// incomingTransField.clone();

									logger.debug("Construct output field " + outputTransField.getName() + " ("
											+ outputTransField.getDataType() + " : " + outputTransField.getPrecision()
											+ ")");
									outputTransField.setName(incomingFieldName);
									TransformField outTransTField = new TransformField(outputTransField,
											PortType.OUTPUT);

									outTransTField.setExpr(incomingTransField.getName());

									outFieldsForDummyExpr.add(outTransTField);
									logger.debug("--> add (" + outputTransField.getName() + " --> "
											+ metadataField.getName() + ") into out Fields of DummyExpr");

								} else {
									logger.error("Can't find the in identifier " + inIdentifier + " rowset");
								}
							} else {

								if (opType.equals("CONSTANT")) {

									logger.debug("Found CONSTANT operator connecting to Union. The union field will be added as transform field in dummy expression");
									TransformField outTransTField = new TransformField(incomingTransField,
											PortType.OUTPUT);

									outTransTField.setExpr(expression);

									outFieldsForDummyExpr.add(outTransTField);

									logger.debug("Add transform field " + incomingTransField.getName()
											+ " into dummy expression");
								} else {
									logger.debug("The operaor " + inOperator + " (" + opType
											+ ") is not supported.Failed to connect to transform field " + inField
											+ ". Origin expression is : " + expression);

								}

							}

							processedFields.add(incomingFieldName);

						} else {

							logger.debug(inField + " of " + inGroup + " of " + inOperator + " has been added into list");
						}

					}
				}
				for (String iden : inputsTemp.keySet()) {
					PortPropagationContext linkPropContext = PortPropagationContextFactory
							.getContextForIncludeCols(inputsTemp.get(iden).getIncludeFields());
					PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(inputsTemp.get(iden)
							.getExprLink());

					InputSet inSet = new InputSet(inputsTemp.get(iden).getRs(), linkPropContext, linkContext);

					allInputSetsForGroup.add(inSet);
					logger.debug("--> construct inputset for in identifier " + iden
							+ " and add into allInputSetsForGroup");
				}

				if (allInputSetsForGroup.size() > 0) {

					logger.debug("--->allInputSetsForGroup : " + allInputSetsForGroup.size() + " elements");
					logger.debug("--->outFieldsForDummyExpr : " + outFieldsForDummyExpr.size() + " elements");

					RowSet epxRS = helper.expression(allInputSetsForGroup, outFieldsForDummyExpr, exprName)
							.getRowSets().get(0);

					INFAUtils.addToExpRowSetHM(expInputSetHM, exprName, epxRS);

					logger.debug("Construct the dummy expression " + exprName + " for UNION transform group : "
							+ inComingGroup.getName());
				}
			} else {
				logger.debug("Skip this group " + inComingGroup.getName() + " because it's out group");
			}

		}

		// Get the inputsets

		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

		ArrayList<Field> includeFields = new ArrayList<Field>();
		ArrayList<Field> linkFields = new ArrayList<Field>();

		for (MetadataGroup inComingGroup : allGroups) {

			if (inComingGroup.getDirection().equals("INGRP")) {

				// Initial the container data structure
				includeFields.clear();
				linkFields.clear();

				String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION")
						+ INFAPropConstants.getINFATransPrefix("UNION") + inComingGroup.getName();

				RowSet expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, exprName);

				if (expRS != null) {

					Iterator<String> itIncludeFieldNames = outputFieldNames.iterator();
					// Hashtable<Field, Object> exprLink = new Hashtable<Field,
					// Object>();

					while (itIncludeFieldNames.hasNext()) {
						String incomingFieldName = itIncludeFieldNames.next();
						// MetadataField metadataField =
						// inComingGroup.getFieldByName(incomingFieldName);
						//
						// Field incomingTransField =
						// INFAUtils.convertToField(metadataField,
						// FieldType.TRANSFORM);

						Field outField = expRS.getField(incomingFieldName);

						includeFields.add(outField);

						// linkFields.add(incomingTransField);

						// exprLink.put(outField, incomingTransField);
						logger.debug("Add " + incomingFieldName);
					}

					PortPropagationContext linkPropContext = PortPropagationContextFactory
							.getContextForIncludeCols(includeFields);
					// PortLinkContext linkContext =
					// PortLinkContextFactory.getPortLinkContextByMap(exprLink);
					// PortLinkContext linkContext =
					// PortLinkContextFactory.getPortLinkContextByPosition(linkFields);

					// InputSet inSet = new InputSet(expRS, linkPropContext,
					// linkContext);
					InputSet inSet = new InputSet(expRS, linkPropContext);
					allInputSets.add(inSet);

					logger.debug(" *** Get output rowset from " + exprName + " with " + includeFields.size()
							+ " elements");
				}
			}
		}

		// Generate output group
		RowSet groupRowSet = new RowSet();
		String outputGroupName = transOP.getOutputGroupNames().get(0);

		if (outputGroupName != null) {
			groupRowSet.setName(outputGroupName);

			MetadataGroup outGroup = transOP.getGroupByName(outputGroupName);

			if (outGroup != null) {

				Iterator<String> itIncludeFieldNames = outputFieldNames.iterator();

				while (itIncludeFieldNames.hasNext()) {
					String incomingFieldName = itIncludeFieldNames.next();

					MetadataField outField = outGroup.getFieldByName(incomingFieldName);

					Field returnField = INFAUtils.convertToField(outField, FieldType.TRANSFORM);
					groupRowSet.addField(returnField);

					logger.debug("** Add " + incomingFieldName + " to return group");
				}

				String unionName = INFAPropConstants.getINFATransPrefix("UNION") + transName;

				UnionTransformation unionTrans = new UnionTransformation(unionName, unionName, unionName, unionName,
						mapping, allInputSets, groupRowSet, null);

				RowSet unionRS = unionTrans.apply().getRowSets().get(0);

				logger.debug("The union " + unionName + " has been added into mapping");

				INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, unionRS);

				logger.debug("Union operator " + unionRS + "(" + identifier + ") has been added as a UNION transform "
						+ "and the rowset of UNION transform has been added into memory hashmap.");

				INFAValidator.validateTransformInstance(unionTrans);

			}

		} else {

			logger.error("Can't find the output group. Failed to add the transform");

			return;

		}

		HelperBase.printSplitLine(logger);

	}

	public void addAggrTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Aggregator transform");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();

		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for Aggregator " + transName
				+ " have been processed and added into export set");

		// TransformHelper helper = new TransformHelper(mapping);

		// Get inputs
		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);

		InputSet inputSet;

		/*
		 * Check if there are multiple input set for this aggregator. If more
		 * than two input sets connecting to the aggrator, then introduce a
		 * expression before the sorter
		 */

		if (allInputSets.size() > 0) {
			String exprName = INFAPropConstants.getINFATransPrefix("UNSUPPORTED") + transName;
			ArrayList<TransformField> transformFields = new ArrayList<TransformField>();

			ExpTransformation exprTrans = new ExpTransformation(exprName, exprName,
					"Dummy Expression Transformation for OWB operator ", exprName, mapping, allInputSets,
					transformFields, null);

			logger.debug("Add Expression transform  " + exprName + " into the mapping");

			INFAValidator.validateTransformInstance(exprTrans);

			RowSet exprRS = exprTrans.apply().getRowSets().get(0);

			inputSet = new InputSet(exprRS);
		} else {
			inputSet = allInputSets.get(0);

		}

		INFAValidator.validateInputSet(inputSet);

		String groupClause = OWBPropConstants.getOWBOPProperty(transOP, "GROUP_BY_CLAUSE");
		String havingClause = OWBPropConstants.getOWBOPProperty(transOP, "HAVING_CLAUSE");

		// Generate the group array
		// Default the prefix as INGRP1.
		// TODO: Add more common replace codes
		groupClause = groupClause.replace("INGRP1.", "");
		String[] groupIDs = groupClause.split(",");

		int keysCnt = groupIDs.length;

		for (int i = 0; i < keysCnt; i++) {
			groupIDs[i] = groupIDs[i].trim();
		}

		logger.debug("Get the gorup id list : " + Arrays.toString(groupIDs));

		boolean[] sortKyesAscending = new boolean[keysCnt];

		// Construct the prior sorter transform for aggregator

		String sorterName = INFAPropConstants.getINFATransPrefix("SORT") + "Before_" + transName;

		// use the default value of boolean array elements

		OutputSet sorterOS = helper.sorter(inputSet, groupIDs, sortKyesAscending, sorterName);

		RowSet sorterRS = sorterOS.getRowSets().get(0);

		// Get transform field list
		String outputGroupName = transOP.getOutputGroupNames().get(0);

		List<TransformField> transformFields = new ArrayList<TransformField>();

		Hashtable<String, String> aggrOutputMapping = new Hashtable<String, String>();

		if (outputGroupName != null) {

			MetadataGroup outGroup = transOP.getGroupByName(outputGroupName);

			if (outGroup != null) {

				ArrayList<MetadataField> outFields = outGroup.getFields();

				for (MetadataField outField : outFields) {

					String aggrFieldName = outField.getName();

					boolean isGroupField = INFAUtils.isFieldExisted(groupIDs, aggrFieldName);

					if (!isGroupField) {
						logger.debug("Field " + aggrFieldName + " is not in the group field list");

						String expression = outField.getExpression();
						String exprType = outField.getData_type();

						logger.debug("...Ready to parse the AGGR output field (" + aggrFieldName + ") expression ("
								+ expression + ")");
						// TODO: Add more common replace codes
						expression = expression.replace("INGRP1.", "");

						Field returnField = INFAUtils.convertToField(outField, FieldType.TRANSFORM);

						String newAggreFieldName = aggrFieldName + "_OUT";

						returnField.setName(newAggreFieldName);

						logger.debug("Rename the output field " + aggrFieldName + " to " + returnField.getName());

						aggrOutputMapping.put(newAggreFieldName, aggrFieldName);

						TransformField tField = new TransformField(returnField, PortType.OUTPUT);

						tField.setExpr(expression);

						tField.setExpressionType(exprType);

						logger.debug("..Set the AGGR output transform field (" + returnField.getName()
								+ ") expression (" + tField.getExpr() + ") type (" + exprType + ")");

						transformFields.add(tField);

						logger.debug("Add " + returnField.getName() + " of " + outGroup.getName() + " of "
								+ transOP.getName() + " into AGGREGATOR transform Field list");
					} else {

						aggrOutputMapping.put(aggrFieldName, aggrFieldName);

					}
				}

				String aggrName = INFAPropConstants.getINFATransPrefix("AGGR") + transName;

				InputSet sorterInputSet = new InputSet(sorterRS);

				allInputSets.clear();
				allInputSets.add(sorterInputSet);

				AggregateTransformation aggrTrans = new AggregateTransformation(aggrName, aggrName, aggrName, aggrName,
						mapping, allInputSets, transformFields, groupIDs, null);

				RowSet aggrRS = aggrTrans.apply().getRowSets().get(0);

				aggrTrans.getTransformationProperties().setProperty("Sorted Input", "YES");

				logger.debug("The AGGREGATOR transform " + aggrName + " as well as the sorter transform " + sorterName
						+ " has been added into mapping");

				// Add post Expression tranform for aggregator

				ArrayList<Field> includeFields = new ArrayList<Field>();
				Hashtable<Field, Object> exprLink = new Hashtable<Field, Object>();
				ArrayList<TransformField> exprOutFields = new ArrayList<TransformField>();

				for (String aggrOutputFieldName : aggrOutputMapping.keySet()) {

					Field aggrOutField = aggrRS.getField(aggrOutputFieldName);

					includeFields.add(aggrOutField);

					String exprFieldName = aggrOutputMapping.get(aggrOutputFieldName);

					MetadataField mField = outGroup.getFieldByName(exprFieldName);

					Field exprField = INFAUtils.convertToField(mField, FieldType.TRANSFORM);

					TransformField tExprField = new TransformField(exprField, PortType.OUTPUT);

					tExprField.setExpr(mField.getExpression());

					exprOutFields.add(tExprField);

					exprLink.put(aggrOutField, exprField);
				}

				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(includeFields);
				PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(exprLink);

				InputSet dummyInputSet = new InputSet(aggrRS, linkPropContext, linkContext);

				if (havingClause != null && havingClause.length() > 0) {

					// TODO: The having clause needs rewritten. Maybe additional
					// field requires to be imported.
					havingClause = havingClause.replace("INGRP1.", "");

					logger.debug("Found the having clause :" + havingClause);

					String filterName = INFAPropConstants.getINFATransPrefix("FILTER")
							+ INFAPropConstants.getINFATransPrefix("AGGR") + transName;

					RowSet filterRS = (RowSet) helper.filter(dummyInputSet, havingClause, filterName).getRowSets()
							.get(0);

					INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, filterRS);

					logger.debug("Transform operator "
							+ aggrName
							+ "("
							+ identifier
							+ ") has been added as a SORTER+AGGREGATOR+FILTER transform because of non-empty having clause "
							+ "and the rowset of FILTER transform has been added into memory hashmap.");

				} else {

					allInputSets.clear();
					allInputSets.add(dummyInputSet);

					String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION")
							+ INFAPropConstants.getINFATransPrefix("AGGR") + transName;

					RowSet epxRS = helper.expression(allInputSets, exprOutFields, exprName).getRowSets().get(0);

					INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, epxRS);

					logger.debug("Transform operator " + aggrName + "(" + identifier
							+ ") has been added as a SORTER+AGGREGATOR+EXPRESSION transform module "
							+ "and the rowset of EXPRESSION transform has been added into memory hashmap.");

					logger.debug("Validate the post expression transform after Aggregator " + aggrName);
					INFAValidator.validateFieldList(epxRS.getFields());
				}

			}

		} else {

			logger.error("Can't find the output group. Failed to add the transform");

			return;

		}

		HelperBase.printSplitLine(logger);

	}

	public void addPreMappingSP(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		String transName = transOP.getName();
		String functionName = "OWBRUNTARGET_DW." + transName;

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Store Procedure transform to replace the OWB pre-mapping operator " + transName);
		HelperBase.printSplitLine(logger);

		transName = INFAUtils.toCamelCase(transName);

		String dummySPCall = functionName + "(";
		RowSet storeRS = new RowSet();

		ArrayList<MetadataGroup> groups = transOP.getGroups();
		if (groups != null && groups.size() > 0) {
			logger.debug("Add input parameters");
			for (MetadataGroup group : groups) {
				if (group.getDirection().equals("INGRP")) {
					ArrayList<MetadataField> fields = group.getFields();

					Iterator<MetadataField> itField = fields.iterator();

					while (itField.hasNext()) {
						MetadataField field = itField.next();
						Field inField = INFAUtils.convertToField(field, FieldType.TRANSFORM);
						storeRS.addField(inField);

						dummySPCall = dummySPCall + inField.getName();

						if (itField.hasNext()) {
							dummySPCall = dummySPCall + ",";
						}
					}

				}
			}
		}

		dummySPCall = dummySPCall + ")";

		InputSet storeInput = new InputSet(storeRS);

		List<TransformField> spTransformFields = new ArrayList<TransformField>();

		String spName = INFAPropConstants.getINFATransPrefix("FUNCTION") + "PreMapping_" + transName;

		helper.storedProc(storeInput, spTransformFields, spName, functionName);

		logger.debug("Add pre-mapping store procedure transform " + spName);

		Transformation spTransform = mapping.getTransformation(spName);

		if (spTransform != null) {

			INFAValidator.validateTransformProperties(spTransform.getTransformationProperties());
			spTransform.getTransformationProperties().setProperty("Stored Procedure Type", "Source Pre Load");
			logger.debug("Set call text as : " + dummySPCall);

			spTransform.getTransformationProperties().setProperty("Stored Procedure Name", functionName);

			logger.debug("Set stored procedure name as : " + functionName);

			logger.debug("Set transform properties for " + spName);
		} else {

			logger.error("Failed to find the transform " + spName + " from mapping object");
		}
	}

	public void addPostMappingSP(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {

		String transName = transOP.getName();
		String functionName = "OWBRUNTARGET_DW." + transName;

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Store Procedure transform to replace the OWB post-mapping operator " + transName);
		HelperBase.printSplitLine(logger);

		transName = INFAUtils.toCamelCase(transName);

		String dummySPCall = functionName + "(";
		RowSet storeRS = new RowSet();

		ArrayList<MetadataGroup> groups = transOP.getGroups();
		if (groups != null && groups.size() > 0) {
			logger.debug("Add input parameters");
			for (MetadataGroup group : groups) {
				if (group.getDirection().equals("INGRP")) {
					ArrayList<MetadataField> fields = group.getFields();

					Iterator<MetadataField> itField = fields.iterator();

					while (itField.hasNext()) {
						MetadataField field = itField.next();
						Field inField = INFAUtils.convertToField(field, FieldType.TRANSFORM);
						storeRS.addField(inField);

						dummySPCall = dummySPCall + inField.getName();

						if (itField.hasNext()) {
							dummySPCall = dummySPCall + ",";
						}
					}

				}
			}
		}

		dummySPCall = dummySPCall + ")";

		InputSet storeInput = new InputSet(storeRS);

		List<TransformField> spTransformFields = new ArrayList<TransformField>();

		String spName = INFAPropConstants.getINFATransPrefix("FUNCTION") + "PostMapping_" + transName;

		helper.storedProc(storeInput, spTransformFields, spName, functionName);

		logger.debug("Add post-mapping store procedure transform " + spName);

		Transformation spTransform = mapping.getTransformation(spName);

		if (spTransform != null) {

			INFAValidator.validateTransformProperties(spTransform.getTransformationProperties());
			spTransform.getTransformationProperties().setProperty("Stored Procedure Type", "Target Post Load");
			spTransform.getTransformationProperties().setProperty("Call Text", dummySPCall);

			logger.debug("Set call text as : " + dummySPCall);

			spTransform.getTransformationProperties().setProperty("Stored Procedure Name", functionName);

			logger.debug("Set stored procedure name as : " + functionName);

			logger.debug("Set transform properties for " + spName);
		} else {

			logger.error("Failed to find the transform " + spName + " from mapping object");
		}
	}

	// This method is to deal with the transform operator in OWB. Replaced with
	// Store Procedure Transform
	public void addFunctionTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		String transName = transOP.getName();

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add Store Procedure and Expression transform to replace the OWB Transform operator "
				+ transName);
		HelperBase.printSplitLine(logger);

		String functionName = "OWBRUNTARGET_DW." + transName;

		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		// Check the Transform operator is standalone or accepting input
		// parameters

		HashSet<String> incomings = transOP.getIncomingRelationships();

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);
			HelperBase.printSplitLine(logger);
			return;
		}

		if (incomings == null || incomings.size() == 0) {
			logger.debug("This transform operator has no input parameter. It's standalone. So we will skip it");
			HelperBase.printSplitLine(logger);
			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for Function(Transform) " + transName
				+ " have been processed and added into export set");

		// TransformHelper helper = new TransformHelper(mapping);

		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);

		ArrayList<TransformField> expTransformFields = new ArrayList<TransformField>();
		ArrayList<TransformField> spTransformFields = new ArrayList<TransformField>();

		RowSet storeRS = new RowSet();

		String dummySPCall = ":SP." + functionName + "(";

		for (MetadataGroup group : transOP.getGroups()) {
			if (group.getDirection().equals("INGRP")) {
				ArrayList<MetadataField> fields = group.getFields();

				Iterator<MetadataField> itField = fields.iterator();

				while (itField.hasNext()) {
					MetadataField field = itField.next();
					Field inField = INFAUtils.convertToField(field, FieldType.TRANSFORM);
					storeRS.addField(inField);

					dummySPCall = dummySPCall + inField.getName();

					if (itField.hasNext()) {
						dummySPCall = dummySPCall + ",";
					}
				}
				dummySPCall = dummySPCall + ")";
			}
		}

		logger.debug("Construct the dummy store procedure call as : " + dummySPCall);

		for (MetadataGroup group : transOP.getGroups()) {
			if (group.getDirection().equals("OUTGRP")) {

				ArrayList<MetadataField> fields = group.getFields();

				Iterator<MetadataField> itField = fields.iterator();

				boolean foundReturnField = false;

				while (itField.hasNext()) {
					MetadataField field = itField.next();

					if (!foundReturnField) {
						foundReturnField = true;

						Field returnField = INFAUtils.convertToField(field, FieldType.TRANSFORM);
						TransformField tReturnField = new TransformField(returnField, PortType.RETURN_OUTPUT);

						spTransformFields.add(tReturnField);

						logger.debug("Add dummy return value field");
					}

					if (foundReturnField) {
						Field outField = INFAUtils.convertToField(field, FieldType.TRANSFORM);
						TransformField tOutField = new TransformField(outField, PortType.OUTPUT);
						tOutField.setExpr(dummySPCall);

						expTransformFields.add(tOutField);

						logger.debug("Add output field " + outField.getName() + " to expression transform");
					}
				}

			}
		}

		InputSet storeInput = new InputSet(storeRS);

		String spName = INFAPropConstants.getINFATransPrefix("FUNCTION") + transName;

		helper.storedProc(storeInput, spTransformFields, spName, functionName);

		logger.debug("Add store procedure transform " + spName);

		Transformation spTransform = mapping.getTransformation(spName);

		if (spTransform != null) {
			spTransform.getTransformationProperties().setProperty("Stored Procedure Name", functionName);
			logger.debug("Set stored procedure name as : " + functionName);
		}

		String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + "For_" + spName;

		RowSet exprRS = helper.expression(allInputSets, expTransformFields, exprName).getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, exprRS);

		logger.debug("Transform operator " + transName + "(" + identifier
				+ ") has been added as a store procedure + expression transforms "
				+ "and the rowset of expression transform has been added into memory hashmap.");

		logger.trace("Validate the store procedure output rowset ");
		for (Field f : exprRS.getFields()) {
			logger.trace(f.getName());
		}
		HelperBase.printSplitLine(logger);

	}

	// The UNPIVOT operator in OWB looks like the SQL pivot operation that is to
	// change row to column
	public void addTransformForUnpivot(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add transforms for unpivot operator conversion");
		HelperBase.printSplitLine(logger);

		// TransformHelper helper = new TransformHelper(mapping);

		String transName = transOP.getName();

		transName = INFAUtils.toCamelCase(transName);

		String outGroupName = transOP.getOutputGroupNames().get(0);
		String identifier = transOP.getName() + "." + outGroupName;

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);

			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for UNPIVOT " + transName
				+ " have been processed and added into export set");

		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);

		String rowLocator = null;
		String[] rowLocatorValues;
		HashMap<Integer, String> rowLocatorMap = new HashMap<Integer, String>();
		ArrayList<String> groupKeyList = new ArrayList<String>();
		HashSet<String> groupKeySet = new HashSet<String>();

		logger.debug("Get properties from input group ...");

		ArrayList<MetadataGroup> groups = transOP.getGroups();
		for (MetadataGroup group : groups) {
			if (group.getDirection().equals("INGRP")) {
				ArrayList<MetadataProperty> groupProperties = group.getProperties();

				for (MetadataProperty groupProperty : groupProperties) {

					if (groupProperty.getName().equals("ROW_LOCATOR")) {
						rowLocator = groupProperty.getValue();

						logger.debug("Get group property ROW_LOCATOR as : " + rowLocator);

						rowLocator = INFAUtils.extractSecondPart(rowLocator);

						logger.debug("After remove the first part before dot, the ROW_LOCATOR value is " + rowLocator);
					} else if (groupProperty.getName().equals("ROW_LOCATOR_VALUES")) {

						String originalValue = groupProperty.getValue();

						logger.debug("Get group property ROW_LOCATOR_VALUES as : " + originalValue);

						rowLocatorValues = originalValue.split(",");

						logger.debug("After spliting the ROW_LOCATOR_VALUES with comma, the string array contains");

						int index = 1;

						for (String rowValue : rowLocatorValues) {
							logger.debug(index + ":\t" + rowValue);

							rowLocatorMap.put(index, rowValue);

							index++;
						}
					}

				}

				logger.debug("Get group keys from input fields...");

				ArrayList<MetadataField> allInputFields = group.getFields();

				for (MetadataField inputField : allInputFields) {

					ArrayList<MetadataProperty> fieldProperties = inputField.getProperties();

					for (MetadataProperty fieldProperty : fieldProperties) {

						if (fieldProperty.getName().equals("GROUP_KEY")) {

							if (fieldProperty.getValue().equals("true")) {

								String groupKeyName = inputField.getName();
								groupKeyList.add(groupKeyName);

								groupKeySet.add(groupKeyName);

								logger.debug("\tFound group key " + groupKeyName);
							}
						}
					}
				}
			}
		}

		logger.debug("The properties in input group have been processed");

		String[] groupKeys = groupKeyList.toArray(new String[0]);

		int keysCnt = groupKeys.length;

		boolean[] sortKyesAscending = new boolean[keysCnt];

		RowSet sortRS = null;

		if (allInputSets.size() > 1) {

			logger.debug("There are more than one input sets for unpivot operator. So a module of expression+sorter+aggregator will be used");

			String exprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + "Collect_Inputs_For_" + transName;

			List<TransformField> transformFields = new ArrayList<TransformField>();
			RowSet expRS = helper.expression(allInputSets, transformFields, exprName).getRowSets().get(0);

			String sortName = INFAPropConstants.getINFATransPrefix("SORT") + "Before_" + transName;
			sortRS = helper.sorter(expRS, groupKeys, sortKyesAscending, sortName).getRowSets().get(0);

		} else {

			logger.debug("There is only one input set for unpivot operator. So a module of sorter+aggregator will be used");

			String sortName = INFAPropConstants.getINFATransPrefix("SORT") + "for_"
					+ INFAPropConstants.getINFATransPrefix("AGGR") + transName;

			InputSet inputSet = allInputSets.get(0);

			sortRS = helper.sorter(inputSet, groupKeys, sortKyesAscending, sortName).getRowSets().get(0);

		}

		logger.debug("The SORTER transform has been constructed");

		logger.debug("To construct the aggreator transform ...");

		ArrayList<TransformField> aggrTransFields = new ArrayList<TransformField>();

		//		String rowLocatorDateType = sortRS.getField(rowLocator).getDataType();

		// Because the output fields in unpivot operator won't have the same
		// same as the original fields, so we don't the naming conversion.
		for (MetadataGroup group : groups) {
			if (group.getDirection().equals("OUTGRP")) {

				ArrayList<MetadataField> allOutputFields = group.getFields();

				for (MetadataField outputField : allOutputFields) {

					String outputFieldName = outputField.getName();

					Field outField = INFAUtils.convertToField(outputField, FieldType.TRANSFORM);

					TransformField tOutField = new TransformField(outField, PortType.OUTPUT);

					logger.debug("To generate expression for unpivot output field " + outputField.getName());

					if (groupKeySet.contains(outputFieldName)) {

						// aggrTransFields.add(tOutField);

						logger.debug("The field " + outputFieldName + " is group key. skit it");
					} else {
						String unpivotExpression = null;

						String machingRow = null;

						ArrayList<MetadataProperty> fieldProperties = outputField.getProperties();

						for (MetadataProperty fieldProperty : fieldProperties) {

							if (fieldProperty.getName().equals("UNPIVOT_EXPRESSION")) {

								unpivotExpression = fieldProperty.getValue();

								unpivotExpression = INFAUtils.extractSecondPart(unpivotExpression);
							} else if (fieldProperty.getName().equals("MATCHING_ROW")) {

								machingRow = fieldProperty.getValue().trim();

							}
						}

						String matchingValue = rowLocatorMap.get(Integer.parseInt(machingRow));

						String finalExpression;

						//Fix the pivot expression, 9/24/2014
						finalExpression = "MAX(" + unpivotExpression + "," + rowLocator + " = " + matchingValue + ")";

						tOutField.setExpr(finalExpression);

						aggrTransFields.add(tOutField);

						logger.debug("The final expression for field " + outputFieldName + " is : " + finalExpression);
					}
				}

			}
		}

		logger.debug("Add the aggregator transform into mapping");

		String aggrName = INFAPropConstants.getINFATransPrefix("AGGR") + transName;

		InputSet sorterInputSet = new InputSet(sortRS);

		allInputSets.clear();
		allInputSets.add(sorterInputSet);

		AggregateTransformation aggrTrans = new AggregateTransformation(aggrName, aggrName, aggrName, aggrName,
				mapping, allInputSets, aggrTransFields, groupKeys, null);

		RowSet aggrRS = aggrTrans.apply().getRowSets().get(0);

		aggrTrans.getTransformationProperties().setProperty("Sorted Input", "YES");

		logger.debug("The AGGREGATOR transform " + aggrName
				+ " has been added into mapping to replace the unpivot operator in OWB");

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, aggrRS);

		logger.debug("Transform operator " + aggrName + "(" + identifier + ") has been added into memory hashmap.");

		HelperBase.printSplitLine(logger);
	}

	public void addDummyTransformForPlaceHolder(MetadataOperator transOP, Folder folder, Repository rep,
			Mapping mapping, MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException, RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add dummy Expression transform for place holder for unsupported OWB operator");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();
		transName = INFAUtils.toCamelCase(transName);
		ArrayList<String> outGroupNames = transOP.getOutputGroupNames();

		String identifier = transOP.getName() + "." + outGroupNames.get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);

			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for unsupported transform " + transName
				+ " have been processed and added into export set");

		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);
		ArrayList<TransformField> transformFields = new ArrayList<TransformField>();
		for (String outGroupName : outGroupNames) {

			MetadataGroup outGroup = transOP.getGroupByName(outGroupName);

			if (outGroup != null) {

				ArrayList<MetadataField> outFields = outGroup.getFields();

				for (MetadataField outField : outFields) {

					String expression = outField.getExpression();

					Field outputTransField = INFAUtils.convertToField(outField, FieldType.TRANSFORM);

					TransformField tField = new TransformField(outputTransField, PortType.OUTPUT);

					tField.setExpr("-- This field is from unsupported OWB operator. Origin expression is : "
							+ expression);
					transformFields.add(tField);
				}
			}

		}

		logger.debug("Finish the unsupported transform field processing");

		String exprName = INFAPropConstants.getINFATransPrefix("UNSUPPORTED") + transName;

		ExpTransformation exprTrans = new ExpTransformation(exprName, transName,
				"Dummy Expression Transformation for OWB operator ", exprName, mapping, allInputSets, transformFields,
				null);

		logger.debug("Add Expression transform  " + exprName + " into the mapping");

		RowSet exprRS = exprTrans.apply().getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, exprRS);
		logger.debug("Add the export rowset of Dummy Expression transform  " + exprName + " into the hashmap memory");

		HelperBase.printSplitLine(logger);
	}

	public void addSorterTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add SORTER transform");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();
		transName = INFAUtils.toCamelCase(transName);

		String identifier = transOP.getName() + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);

			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for SORTER " + transName
				+ " have been processed and added into export set");

		String orderByClause = null;

		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);

		InputSet inputSet = allInputSets.get(0);

		MetadataGroup group = transOP.getGroups().get(0);

		ArrayList<MetadataProperty> opProperties = transOP.getProperties();

		if (opProperties != null) {
			for (MetadataProperty prop : opProperties) {
				if (prop.getName().equals("ORDER_BY_CLAUSE")) {
					orderByClause = prop.getValue();

					logger.debug("Found order_by_clause : " + orderByClause);
				}
			}
		}
		if (orderByClause != null) {
			orderByClause = orderByClause.replace("INOUTGRP1.", "");
		} else {
			orderByClause = group.getFields().get(0).getName();

			logger.debug("Can't find the order_by_clause hence get random field as order id : " + orderByClause);
		}

		String[] sortTerms = orderByClause.split(",");

		// Construct the prior sorter transform for aggregator

		String sorterName = INFAPropConstants.getINFATransPrefix("SORT") + transName;
		int keysCnt = sortTerms.length;

		boolean[] sortKyesAscending = new boolean[keysCnt];

		String[] sortIDs = new String[keysCnt];

		logger.trace("Parsing the sort terms array (" + keysCnt + ")");

		for (int i = 0; i < keysCnt; i++) {
			String sortTerm = sortTerms[i].trim();

			logger.trace("Get sort term : " + sortTerm);

			String[] keyTerms = sortTerm.split(" ");
			logger.trace("Get key terms array (" + keyTerms.length + ")");

			String key = keyTerms[0];

			String sortDirect = keyTerms[1];

			logger.trace("Find the key :" + key + ", sort direction : " + sortDirect);
			sortIDs[i] = key;

			if (sortDirect.equals("ASC")) {
				sortKyesAscending[i] = true;
			} else {
				sortKyesAscending[i] = false;
			}

		}

		// use the default value of boolean array elements
		// TransformHelper helper = new TransformHelper(mapping);

		OutputSet sorterOS = helper.sorter(inputSet, sortIDs, sortKyesAscending, sorterName);

		RowSet sorterRS = sorterOS.getRowSets().get(0);

		INFAUtils.addToExpRowSetHM(expInputSetHM, identifier, sorterRS);

		logger.debug("Sorter " + transName + "(" + identifier
				+ ") has been added as a SORTER transform and into memory hashmap.");

		HelperBase.printSplitLine(logger);
	}

	public void addRouterTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws InvalidTransformationException,
			RepoOperationException, MapFwkReaderException {
		HelperBase.printSplitLine(logger);
		logger.debug("Ready to add ROUTER transform");
		HelperBase.printSplitLine(logger);

		String transName = transOP.getName();
		String originalTransName = transName;

		transName = INFAUtils.toCamelCase(transName);

		String identifier = originalTransName + "." + transOP.getOutputGroupNames().get(0);

		if (isOperatorProcessed(expInputSetHM, identifier)) {
			logger.debug("The operator has been processed. Skip processing " + transName);

			return;
		}

		checkAndInstallAllRelatesUptoDQ(transOP, folder, rep, mapping, owbMapping, expInputSetHM);

		logger.debug("All the required operators for ROUTER " + transName
				+ " have been processed and added into export set");

		ArrayList<InputSet> allInputSets = getAllInputSets(transOP, owbMapping, expInputSetHM);

		List<TransformGroup> transformGrps = new ArrayList<TransformGroup>();

		int serialNo = 1;

		HashMap<String, HashMap<String, String>> outputMap = new HashMap<String, HashMap<String, String>>();

		HashMap<String, String> groupNameMap = new HashMap<String, String>();

		for (MetadataGroup group : transOP.getGroups()) {
			String groupDirection = group.getDirection();
			String groupName = group.getName();
			if (groupDirection.equals("OUTGRP") && !groupName.equals("REMAINING_ROWS")) {
				String groupCondition = null;
				ArrayList<MetadataProperty> opProperties = transOP.getProperties();

				if (opProperties != null) {
					for (MetadataProperty prop : opProperties) {
						if (prop.getName().equals("SPLIT_CONDITION")) {
							groupCondition = prop.getValue();

							if (groupCondition == null) {
								groupCondition = "TRUE -- There is no group condition in OWB. Please review OWB production mapping and add it manually.";
							}
							logger.debug("Found order_by_clause : (" + groupCondition + ")");
						}
					}
				}

				TransformGroup transGrp = new TransformGroup(groupName, groupCondition);
				transformGrps.add(transGrp);
				groupNameMap.put(Integer.toString(serialNo), groupName);

				logger.debug("Construct naming expression for ROUTER");

				logger.debug("Construct the input set for the naming expression");
				ArrayList<MetadataField> mFields = group.getFields();
				HashMap<String, String> groupOutputMap = new HashMap<String, String>();

				for (MetadataField mField : mFields) {

					String owbFieldName = mField.getName();
					String rtrFieldName = owbFieldName + serialNo;

					groupOutputMap.put(rtrFieldName, owbFieldName);

					logger.trace(rtrFieldName + "  -->  " + owbFieldName);
				}

				outputMap.put(Integer.toString(serialNo), groupOutputMap);

				logger.debug("Add output field mapping for group" + groupName + " as serial no " + serialNo);
				serialNo++;
			}
		}

		String rtrTransName = INFAPropConstants.getINFATransPrefix("SPLITTER") + transName;

		OutputSet outputSetRtr = helper.router(allInputSets, transformGrps, rtrTransName, null);

		for (String serial : groupNameMap.keySet()) {
			int index = Integer.parseInt(serial) - 1;
			logger.debug("To get index " + index + " of output sets of Router");
			RowSet rtrRS = outputSetRtr.getRowSets().get(index);
			if (rtrRS != null) {

				String groupName = groupNameMap.get(serial);
				String rtrIdentifier = originalTransName + "." + groupName;

				String namingExprName = INFAPropConstants.getINFATransPrefix("EXPRESSION") + "Naming_" + groupName
						+ "_Of_" + transName;

				logger.debug("Costruct the naming expression " + namingExprName + " for group " + groupName
						+ " of Router");

				ArrayList<Field> includeFields = new ArrayList<Field>();
				Hashtable<Field, Object> exprLink = new Hashtable<Field, Object>();

				logger.debug("Construct the input set for the naming expression");

				HashMap<String, String> rtrOutputMap = outputMap.get(serial);

				for (String rtrFieldName : rtrOutputMap.keySet()) {

					Field rtrGroupField = rtrRS.getField(rtrFieldName);

					if (rtrGroupField == null) {
						logger.debug("Can't find field " + rtrFieldName + " in the router group (" + groupName
								+ ") output rowset. Take another try.");

						rtrGroupField = INFAUtils.tryOtherPossibleName(rtrRS, rtrFieldName);

					}

					Field expressionInputField = (Field) rtrGroupField.clone();

					expressionInputField.setName(rtrOutputMap.get(rtrFieldName));

					includeFields.add(rtrGroupField);

					exprLink.put(rtrGroupField, expressionInputField);

					logger.trace("Connect " + rtrFieldName + "  -->  " + expressionInputField.getName());
				}

				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(includeFields);
				PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(exprLink);

				InputSet dummyInputSet = new InputSet(rtrRS, linkPropContext, linkContext);

				ArrayList<InputSet> allIExprnputSets = new ArrayList<InputSet>();

				allIExprnputSets.add(dummyInputSet);

				ArrayList<TransformField> transformFields = new ArrayList<TransformField>();

				RowSet expRS = helper.expression(allIExprnputSets, transformFields, namingExprName).getRowSets().get(0);

				logger.debug("Constructed the naming expression for router group");

				INFAUtils.addToExpRowSetHM(expInputSetHM, rtrIdentifier, expRS);

				logger.debug("Add the export rowset " + groupName + " of Router transform (identifier : "
						+ rtrIdentifier + ") into the hashmap memory");
			} else {
				logger.error("Can't find the rowset indexing as " + index + " in Router rowsets");
			}
		}
	}

	@SuppressWarnings("unused")
	private ArrayList<InputSet> getAllInputSets(MetadataOperator transOP, MetadataMapping owbMapping,
			HashMap<String, RowSet> expInputSetHM) {
		ArrayList<InputSet> allInputSets = new ArrayList<InputSet>();

		HelperBase.printSplitLine(logger);
		logger.debug("Ready to get all the input sets for operator " + transOP.getName());
		HelperBase.printSplitLine(logger);

		logger.trace("The memory hashmap has following entries:");
		for (String key : expInputSetHM.keySet()) {
			logger.trace(key);
		}

		// Get input fields

		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter;
		inIdentifierIter = incomingRelatedOPs.iterator();

		HashSet<String> processedFields = new HashSet<String>();

		ArrayList<Field> includeFields = new ArrayList<Field>();
		Hashtable<Field, Object> exprLink = new Hashtable<Field, Object>();

		while (inIdentifierIter.hasNext()) {

			String inIdentifier = inIdentifierIter.next();

			logger.debug("Start to contruct the input set for incoming identifier " + inIdentifier);

			RowSet expRS = null;

			logger.debug("Get export row set for identifier " + inIdentifier);

			includeFields.clear();
			exprLink.clear();

			ArrayList<MetadataGroup> incomingGroups = transOP.getGroups();

			for (MetadataGroup inComingGroup : incomingGroups) {

				if (inComingGroup.getDirection().matches(".*IN.*")) {
					List<MetadataField> allIncomingFields = inComingGroup.getFields();

					// HashSet<String> processedFields = new
					// HashSet<String>();

					String URI = null;
					for (MetadataField metadataField : allIncomingFields) {
						String incomingFieldName = metadataField.getName();

						logger.debug("Processing the field " + incomingFieldName + " in this transform");
						ArrayList<MetadataConnection> inputConnections = metadataField.getInputConnections();

						if (inputConnections != null && inputConnections.size() > 0) {
							MetadataConnection inConnection = inputConnections.get(0);

							String inField = inConnection.getSourceField();
							String inGroup = inConnection.getSourceGroup();
							String inOperator = inConnection.getSourceOperator();

							logger.debug("Processing the incoming " + inOperator + "." + inGroup + "." + inField);

							MetadataOperator mOP = owbMapping.getOperatorByName(inOperator);
							MetadataGroup mGp = mOP.getGroupByName(inGroup);
							MetadataField mFd = mGp.getFieldByName(inField);

							// String bizName = mOP.getBusiness_name();

							if (inIdentifier.equals(inOperator + "." + inGroup)) {

								logger.debug("The field " + incomingFieldName
										+ " matches the incoming connection from identifier : " + inIdentifier);

								String expression = mFd.getExpression();
								String opType = mOP.getType();

								if (!processedFields.contains(incomingFieldName)) {
									logger.debug("Ready to add the field " + incomingFieldName);

									Field incomingTransField = INFAUtils.convertToField(metadataField,
											FieldType.TRANSFORM);

									if (INFAUtils.isSupportedOWBTransform(opType)) {

										logger.debug("The incomming connection is from supported operator (type : "
												+ opType + ")");

										if (expRS == null) {

											// Initial the rowset
											if (mOP.isSource()) {
												String srcIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE")
														+ inOperator;

												expRS = INFAUtils
														.getExpRowSet(owbMapping, expInputSetHM, srcIdentifier);
												URI = srcIdentifier;
												logger.debug("Get export row set for source " + inOperator);

												List<Field> rsFields = expRS.getFields();
												for (Field rsField : rsFields) {
													logger.trace("\t ... " + rsField.getName());
												}

											} else {
												expRS = INFAUtils.getExpRowSet(owbMapping, expInputSetHM, inIdentifier);
												URI = inIdentifier;
												logger.debug("Get export row set for identifier " + inIdentifier);

												List<Field> rsFields = expRS.getFields();
												for (Field rsField : rsFields) {
													logger.trace("\t ... " + rsField.getName());
												}

											}
										}

										if (expRS != null) {
											// TransformField tField1 = new
											// TransformField(targetField,
											// PortType.INPUT_OUTPUT);

											// transformFields.add(tField1);
											logger.debug("To add " + inField + " of " + inGroup + " of " + inOperator
													+ " into transformField list");

											Field sourcingField = expRS.getField(inField);

											if (sourcingField != null) {
												includeFields.add(sourcingField);
												exprLink.put(sourcingField, incomingTransField);

												logger.debug("Transform Field " + incomingTransField.getName()
														+ " is connected from " + sourcingField.getName() + " of "
														+ inGroup + " of " + inOperator);
											} else {
												logger.error("Can't find the field " + inField
														+ " from upward rowset named as " + URI);
											}
										} else {
											logger.error("Can't find the export RowSet for indentifier " + inIdentifier);
										}
									} else {

										logger.error("The operaor " + inOperator + " (" + opType
												+ ") is not supported.Failed to connect to transform field " + inField
												+ ". Origin expression is : " + expression);

										logger.debug("For compatible,  only create filed from supported operator");

									}

									processedFields.add(incomingFieldName);

								} else {

									logger.debug(inField + " of " + inGroup + " of " + inOperator
											+ " has been added into list");
								}
							} else {

								logger.debug("Bypass the field " + incomingFieldName
										+ " becasue it doesn't match the incoming identifier (" + inIdentifier
										+ ") we are looking for ");
							}
						}
					}

				} else {
					logger.debug("Skip this group because it's out group");
				}
			}
			if (expRS != null) {
				PortPropagationContext linkPropContext = PortPropagationContextFactory
						.getContextForIncludeCols(includeFields);
				PortLinkContext linkContext = PortLinkContextFactory.getPortLinkContextByMap(exprLink);

				InputSet inSet = new InputSet(expRS, linkPropContext, linkContext);

				allInputSets.add(inSet);

			} else {
				logger.error("Can't find the rowset " + inIdentifier);
			}

		}
		return allInputSets;
	}

	public void addSequenceTransform(MetadataOperator transOP, Folder folder, Repository rep, Mapping mapping,
			MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM) throws RepoOperationException,
			MapFwkReaderException {
		logger.debug("--- Ready to add " + transOP.getName() + " as StoreProcedure transform in Informatica mapping");

		final String targetFolderName = "#ETL_SH_UTILS";

		List<Folder> folders = rep.getFolders(new INameFilter() {

			public boolean accept(String name) {
				return name.equals(targetFolderName);
			}
		});

		if (folders.size() > 0) {

			logger.debug("Folder " + targetFolderName + " is found");

			Folder temp = folders.get(0);

			final String targetTransName = "SEQ_NEXTVAL";

			List<Transformation> transformations = temp.fetchTransformationsFromRepository(new INameFilter() {

				public boolean accept(String name) {
					return name.equals(targetTransName);
				}
			});

			if (transformations.size() > 0) {
				Transformation seqTrans = transformations.get(0);

				String repoName = rep.getRepoConnectionInfo().getTargetRepoName();
				ShortCut scTrans = new ShortCut("SEQ_NEXTVAL", "shortcut to Sequence " + transOP.getName(), repoName,
						targetFolderName, "SEQ_NEXTVAL", RepositoryObjectConstants.OBJTYPE_TRANSFORMATION,
						TransformationConstants.STR_STORED_PROC, ShortCut.LOCAL);

				scTrans.setRefObject(seqTrans);

				folder.addShortCut(scTrans);
				// folder.addShortCutInternal(scTrans);

				mapping.addShortCut(scTrans);

				logger.debug("The sequence SQL_NEXTVAL shortcut has been created");
			} else {
				logger.error("Failed to find the transformation " + targetTransName + " in folder " + targetFolderName);
			}
		} else {
			logger.error("Failed to find the folder " + targetFolderName);
		}

	}

	private void checkAndInstallAllRelatesUptoSource(MetadataOperator transOP, Folder folder, Repository rep,
			Mapping mapping, MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException, RepoOperationException, MapFwkReaderException {
		logger.debug("Check if the incoming operators been processed already");
		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter = incomingRelatedOPs.iterator();

		while (inIdentifierIter.hasNext()) {
			String inIdentifier = inIdentifierIter.next();
			String inOPName = INFAUtils.extractFirstPart(inIdentifier);

			if (!INFAUtils.isLoadControlTable(inOPName)) {

				MetadataOperator inOP = owbMapping.getOperatorByName(inOPName);
				if (inOP.isSource()) {
					if (INFAUtils.isTransExpRowSetExisted(expInputSetHM, inOPName)) {
						logger.debug("Source " + inOPName + " has been processed and the export rowset is ready");
					} else {
						logger.debug(inOPName + " hasn't been processed.");

						logger.debug("Continue to add the missing source " + inOPName);

						INFAMapSource infaMapSource = new INFAMapSource();

						// infaMapSource.addIfNotExistedSource(inOP, folder,
						// rep,
						// mapping, owbMapping, expInputSetHM);

						infaMapSource.addSource(inOP, folder, rep, mapping, owbMapping, expInputSetHM);

						HelperBase.printSplitLine(logger);

					}
				} else {
					if (INFAUtils.isTransExpRowSetExisted(expInputSetHM, inIdentifier)) {
						logger.debug(inOPName + " has been processed and the export rowset (" + inIdentifier
								+ ") is ready");
					} else {
						logger.debug(inOPName + " hasn't been processed.");

						logger.debug("Continue to add the missing operator " + inOPName);
						addIfNotExistedTransform(inOP, folder, rep, mapping, owbMapping, expInputSetHM);

						HelperBase.printSplitLine(logger);
					}
				}
			} else {
				logger.debug("Skip the checking of " + inOPName);
			}
		}
	}

	private void checkAndInstallAllRelatesUptoDQ(MetadataOperator transOP, Folder folder, Repository rep,
			Mapping mapping, MetadataMapping owbMapping, HashMap<String, RowSet> expInputSetHM)
			throws InvalidTransformationException, RepoOperationException, MapFwkReaderException {
		logger.debug("Check if the incoming operators been processed already");
		HashSet<String> incomingRelatedOPs = transOP.getIncomingRelationships();
		Iterator<String> inIdentifierIter = incomingRelatedOPs.iterator();

		while (inIdentifierIter.hasNext()) {
			String inIdentifier = inIdentifierIter.next();
			String inOPName = INFAUtils.extractFirstPart(inIdentifier);

			if (!INFAUtils.isLoadControlTable(inOPName)) {

				MetadataOperator inOP = owbMapping.getOperatorByName(inOPName);
				if (inOP.isSource()) {

					inIdentifier = INFAPropConstants.getINFATransPrefix("SOURCE") + inOPName;

					logger.debug("Check DQ " + inIdentifier);

					if (INFAUtils.isTransExpRowSetExisted(expInputSetHM, inIdentifier)) {
						logger.debug("Source " + inOPName + " has been processed and the DQ export rowset is ready");
					} else {
						logger.debug(inOPName + " DQ hasn't been added.");

						if (INFAUtils.isTransExpRowSetExisted(expInputSetHM, inOPName)) {
							logger.debug("The source has been added into memory hashset.Need to add DQ");

							final String checkName = inOPName;
							List<ShortCut> scList = folder.getShortCuts(new INameFilter() {

								public boolean accept(String name) {
									return name.equalsIgnoreCase(checkName);
								}
							});

							INFAMapSource mapSource = new INFAMapSource();

							if (scList != null && scList.size() > 0) {
								logger.debug("The required source has shortcut existing in folder " + folder.getName());

								ShortCut scSource = scList.get(0);

								mapSource.addShortCutAndDSQIntoMap(scSource, mapping, expInputSetHM);
							} else {
								logger.debug("The required source is created locally");

								Source src = folder.getSource(checkName);
								mapSource.addSourceAndDSQIntoMap(inOP, src, folder, mapping, expInputSetHM);
							}
						} else {
							logger.debug("Add the missing source and its DQ " + inOPName);

							INFAMapSource infaMapSource = new INFAMapSource();

							// infaMapSource.addIfNotExistedSource(inOP, folder,
							// rep,
							// mapping, owbMapping, expInputSetHM);

							infaMapSource.addSource(inOP, folder, rep, mapping, owbMapping, expInputSetHM);
						}
					}
				} else {
					if (INFAUtils.isTransExpRowSetExisted(expInputSetHM, inIdentifier)) {
						logger.debug(inOPName + " has been processed and the export rowset (" + inIdentifier
								+ ") is ready");
					} else {
						logger.debug(inOPName + " hasn't been processed.");

						logger.debug("Continue to add the missing operator " + inOPName);
						addIfNotExistedTransform(inOP, folder, rep, mapping, owbMapping, expInputSetHM);

						HelperBase.printSplitLine(logger);
					}
				}
			} else {
				logger.debug("Skip the checking of " + inOPName);
			}
		}
	}

	boolean isOperatorProcessed(HashMap<String, RowSet> expInputSetHM, String identifier) {
		RowSet expRS = INFAUtils.getExpRowSet(expInputSetHM, identifier);
		if (expRS != null) {
			return true;
		} else {
			return false;
		}

	}

}