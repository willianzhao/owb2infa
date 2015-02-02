package org.willian.owb2infa;

import java.io.IOException;
import java.io.InputStream;
import java.io.LineNumberReader;
import java.sql.SQLException;
import java.util.ArrayList;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.willian.owb2infa.helper.HelperBase;
import org.willian.owb2infa.helper.infa.INFAMapConstructor;
import org.willian.owb2infa.helper.owb.OWBMappingParser;

public class MappingMigrator {

	/**
	 * @param args
	 * @throws IOException
	 * @throws IllegalArgumentException
	 */
	public static void main(String[] args) {
		int runMode = -1;
		Logger logger = LogManager.getLogger(MappingMigrator.class);

		MappingMigrator migrator = new MappingMigrator();

		String owbMappingName = null;
		ArrayList<String> owbMappingList = new ArrayList<String>();

		StringBuffer resultSB = new StringBuffer();

		if (args.length > 0) {

			PropertyConfigurator.configure("resource/log4j.properties");

			runMode = Integer.parseInt(args[0]);

			String pattern = "FEEDS.*";

			if (runMode <= 10) {
			//Mapping name will be the same of file name of generated OWB metadata xml
				owbMappingName = "[mapping name]";
				String[] owbMappingArray = { "[mapping name 1]", "[mapping name 2]" };
				for (String task : owbMappingArray) {
					owbMappingList.add(task);
				}
			}

			String XMLDir = "owblog";
			String owbXMLDir = XMLDir + java.io.File.separatorChar + "owbxml";
			String outputPath = XMLDir + java.io.File.separatorChar + "infa";

			logger.debug("************** Mapping Migrator **************");
			long startTimeMillis = System.currentTimeMillis();

			if (runMode == 11) {
				owbMappingName = args[1];

				runMode = 1;
			} else if (runMode == 12) {
				String taskFileName = "task.txt";

				LineNumberReader lineReader = null;
				try {
					lineReader = HelperBase.getLineNumberReader(XMLDir, taskFileName);
				} catch (IOException e) {

					logger.error("Error to open the task.txt file under the folder " + XMLDir);
					logger.error(HelperBase.getStackTrace(e));
					migrator.printUsage(runMode);
					System.exit(1);
				}

				String task = "";
				try {
					while ((task = lineReader.readLine()) != null) {
						owbMappingList.add(task);
					}

					logger.debug("Set the task list for runMode" + runMode);

				} catch (IOException e) {
					logger.error("Error to read the task.txt file under the folder " + XMLDir);
					logger.error(HelperBase.getStackTrace(e));
					migrator.printUsage(runMode);
					System.exit(1);
				}
				runMode = 9;
			} else if (runMode == 13) {
				String taskFileName = "task.txt";

				LineNumberReader lineReader = null;
				try {
					lineReader = HelperBase.getLineNumberReader(XMLDir, taskFileName);
				} catch (IOException e) {

					logger.error("Error to open the task.txt file under the folder " + XMLDir);
					logger.error(HelperBase.getStackTrace(e));
					migrator.printUsage(runMode);
					System.exit(1);
				}

				String task = "";
				try {
					while ((task = lineReader.readLine()) != null) {
						owbMappingList.add(task);
					}

					logger.debug("Set the task list for runMode" + runMode);
				} catch (IOException e) {
					logger.error("Error to read the task.txt file under the folder " + XMLDir);
					logger.error(HelperBase.getStackTrace(e));
					migrator.printUsage(runMode);
					System.exit(1);
				}
				runMode = 10;
			}

			if (runMode == 0) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate OWB xml file)");

				OWBMappingParser owbParser = null;
				try {
					owbParser = new OWBMappingParser(owbXMLDir);
					StringBuffer addSB = new StringBuffer();

					owbParser.parseMapping(owbMappingName, addSB);

					owbParser.gracefulExit();

				} catch (SQLException | IllegalArgumentException | IOException e) {

					logger.error("Failed to generate OWB " + owbMappingName + " xml file");

					logger.error(HelperBase.getStackTrace(e));

					owbParser.gracefulExit();

					System.exit(1);
				}

			} else if (runMode == 1) {

				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Only genearte PowerCenter xml file according to OWB metadata)");

				INFAMapConstructor infaMigrator = null;
				try {
					infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath, owbMappingName);

					if (infaMigrator.validateRunMode(0)) {
						try {
							infaMigrator.execute();
						} catch (Exception e) {
							logger.error("Failed to generate Informatica " + owbMappingName + " xml file");
							logger.error(HelperBase.getStackTrace(e));

							//Runtime.getRuntime().exec("taskkill /T /F /IM pmrep* ");

							System.exit(1);
						}
					}

				} catch (IOException e1) {
					logger.error("Failed to generate Informatica " + owbMappingName
							+ " xml file according to OWB metadata");
					logger.error(HelperBase.getStackTrace(e1));

					System.exit(1);

				}

			} else if (runMode == 2) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Genearte and import PowerCenter xml file into Informatica Repository)");

				INFAMapConstructor infaMigrator;
				try {
					infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath, owbMappingName);

					if (infaMigrator.validateRunMode(1)) {
						try {
							infaMigrator.execute();
						} catch (Exception e) {
							logger.error("Failed to generate and import Informatica " + owbMappingName + " xml file");
							logger.error(HelperBase.getStackTrace(e));
							System.exit(1);
						}
					}

				} catch (IOException e1) {
					logger.error("Failed to genearte and import PowerCenter " + owbMappingName
							+ " xml file into Informatica Repository ");
					logger.error(HelperBase.getStackTrace(e1));
					System.exit(1);
				}

			} else if (runMode == 3) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate OWB and Informatica xml files and then import it to Informatica Repository)");

				OWBMappingParser owbParser = null;
				StringBuffer addSB = new StringBuffer();

				try {

					owbParser = new OWBMappingParser(owbXMLDir);

					try {
						owbParser.parseMapping(owbMappingName, addSB);

						try {
							INFAMapConstructor infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath,
									owbMappingName);
							if (infaMigrator.validateRunMode(1)) {
								try {
									infaMigrator.execute();
								} catch (Exception e) {
									logger.error("Failed to generate and import Informatica " + owbMappingName
											+ " xml file");
									logger.error(HelperBase.getStackTrace(e));
									System.exit(1);
								}
							}
						} catch (IOException e1) {
							logger.error("Failed to create Informatica Paser according to OWB metadata");
							logger.error(HelperBase.getStackTrace(e1));

							System.exit(1);

						}

					} catch (SQLException e) {

						logger.error("Failed to generate OWB " + owbMappingName + " xml file");
						logger.error(HelperBase.getStackTrace(e));
						owbParser.gracefulExit();
						System.exit(1);
					}

				} catch (IllegalArgumentException | IOException e1) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e1));

					System.exit(1);
				}

				owbParser.gracefulExit();

			} else if (runMode == 4) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate OWB and Informatica xml files)");

				OWBMappingParser owbParser = null;
				StringBuffer addSB = new StringBuffer();

				try {

					owbParser = new OWBMappingParser(owbXMLDir);

					try {
						owbParser.parseMapping(owbMappingName, addSB);

						try {
							INFAMapConstructor infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath,
									owbMappingName);
							if (infaMigrator.validateRunMode(0)) {
								try {
									infaMigrator.execute();
								} catch (Exception e) {
									logger.error("Failed to generate Informatica " + owbMappingName + " xml file");
									logger.error(HelperBase.getStackTrace(e));
									System.exit(1);
								}
							}
						} catch (IOException e1) {
							logger.error("Failed to generate Informatica " + owbMappingName
									+ " xml file according to OWB metadata");
							logger.error(HelperBase.getStackTrace(e1));

							System.exit(1);

						}

					} catch (SQLException e) {

						logger.error("Failed to generate OWB " + owbMappingName + " xml file");
						logger.error(HelperBase.getStackTrace(e));
						owbParser.gracefulExit();
						System.exit(1);
					}

				} catch (IllegalArgumentException | IOException e1) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e1));

					System.exit(1);
				}

				owbParser.gracefulExit();

			} else if (runMode == 5) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate OWB mapping xml files with pattern)");

				OWBMappingParser owbParser = null;
				try {
					owbParser = new OWBMappingParser(owbXMLDir);
				} catch (IllegalArgumentException | IOException e) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e));

					System.exit(1);
				}

				// TODO: accept the second input parameter for pattern
				owbParser.parseMappingByPattern(pattern);

				owbParser.gracefulExit();

			} else if (runMode == 6) {
				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate all OWB xml files)");

				OWBMappingParser owbParser = null;
				try {
					owbParser = new OWBMappingParser(owbXMLDir);
				} catch (IllegalArgumentException | IOException e) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e));

					System.exit(1);
				}

				owbParser.parseAllMappings();

				owbParser.gracefulExit();

			} else if (runMode == 7) {
				HelperBase.printSplitLine(logger);
				logger.info("Generate pattern matched OWB and Informatica mapping xml files and import them into Informatica repository");

				OWBMappingParser owbParser = null;
				try {
					owbParser = new OWBMappingParser(owbXMLDir);
				} catch (IllegalArgumentException | IOException e) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e));

					System.exit(1);
				}

				// TODO: accept the second input parameter for pattern

				ArrayList<String> processedMappings = owbParser.parseMappingByPattern(pattern);

				owbParser.gracefulExit();

				if (processedMappings != null && processedMappings.size() > 0) {
					for (String exportedOWBMappingName : processedMappings) {
						INFAMapConstructor infaMigrator = null;
						try {
							infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath, exportedOWBMappingName);

							if (infaMigrator.validateRunMode(1)) {
								try {
									infaMigrator.execute();

								} catch (Exception e) {
									logger.error("Failed to generate and import Informatica " + exportedOWBMappingName
											+ " xml file");
									logger.error(HelperBase.getStackTrace(e));
									System.exit(1);
								}
							}

						} catch (IOException e1) {
							logger.error("Failed to generate Informatica " + exportedOWBMappingName
									+ " xml file according to OWB metadata");
							logger.error(HelperBase.getStackTrace(e1));

							System.exit(1);
						}

					}
				}

			} else if (runMode == 8) {
				HelperBase.printSplitLine(logger);
				logger.info("Generate pattern matched Informatica mapping xml files and import them into Informatica repository");

				OWBMappingParser owbParser = null;
				try {

					owbParser = new OWBMappingParser(owbXMLDir);
				} catch (IllegalArgumentException | IOException e) {
					logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
					logger.error(HelperBase.getStackTrace(e));

					System.exit(1);
				}

				// TODO: accept the second input parameter for pattern

				ArrayList<String> processedMappings = owbParser.getMatchedMappingNames(pattern);
				owbParser.gracefulExit();

				if (processedMappings != null && processedMappings.size() > 0) {

					for (String exportedOWBMappingName : processedMappings) {
						HelperBase.printSplitLine(logger);
						logger.debug("process " + exportedOWBMappingName);

						INFAMapConstructor infaMigrator;
						try {
							infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath, exportedOWBMappingName);

							if (infaMigrator.validateRunMode(1)) {
								try {
									infaMigrator.execute();
								} catch (Exception e) {
									logger.error("Failed to generate and import Informatica " + exportedOWBMappingName
											+ " xml file");
									logger.error(HelperBase.getStackTrace(e));
									System.exit(1);
								}
							}

						} catch (IOException e1) {
							logger.error("Failed to generate Informatica " + exportedOWBMappingName
									+ " xml file according to OWB metadata");
							logger.error(HelperBase.getStackTrace(e1));

							System.exit(1);
						}

					}
				}

			} else if (runMode == 9) {

				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate OWB xml file for serials of mappings)");

				OWBMappingParser owbParser = null;

				if (owbMappingList.size() > 0) {

					resultSB.append("\n");
					try {
						owbParser = new OWBMappingParser(owbXMLDir);
					} catch (IllegalArgumentException | IOException e) {
						logger.error("Failed to create OWB Parser according to the xml file under " + owbXMLDir);
						logger.error(HelperBase.getStackTrace(e));

						System.exit(1);
					}

					StringBuffer addSB = new StringBuffer();

					for (String mappingName : owbMappingList) {
						try {
							owbParser.parseMapping(mappingName, addSB);

							resultSB.append(mappingName + "...... Success\n");
						} catch (Exception e) {

							logger.error("Failed to generate OWB " + mappingName + " xml file");
							logger.error(HelperBase.getStackTrace(e));
							resultSB.append(mappingName + "...... Fail\n");
							continue;
						}
					}

					try {
						owbParser.saveAdditionalInfo(addSB);
					} catch (Exception e) {
						logger.error(HelperBase.getStackTrace(e));
						/*
						 * gracefulExit(); System.exit(1);
						 */
						logger.error("Failed to save OWB mapping additional information");
					}

					owbParser.gracefulExit();
				} else {
					logger.debug("There is no task defined");
				}
			} else if (runMode == 10) {

				HelperBase.printSplitLine(logger);
				logger.info("Start MappingMigrator (Generate Informatica xml file for serials of mappings)");
				if (owbMappingList.size() > 0) {
					resultSB.append("\n");

					for (String exportedOWBMappingName : owbMappingList) {
						HelperBase.printSplitLine(logger);
						logger.debug("process " + exportedOWBMappingName);

						INFAMapConstructor infaMigrator;
						try {
							infaMigrator = new INFAMapConstructor(owbXMLDir, outputPath, exportedOWBMappingName);

							if (infaMigrator.validateRunMode(0)) {
								try {
									infaMigrator.execute();

									//								//Runtime.getRuntime().exec("taskkill /T /F /IM pmrep* ");
									resultSB.append(exportedOWBMappingName + "...... Success\n");
								} catch (Exception e) {
									logger.error("Failed to generate and import Informatica " + exportedOWBMappingName
											+ " xml file");
									logger.error(HelperBase.getStackTrace(e));
									resultSB.append(exportedOWBMappingName + "...... Fail\n");
									continue;
								}
							}

						} catch (IOException e1) {
							logger.error("Failed to generate Informatica " + exportedOWBMappingName
									+ " xml file according to OWB metadata");
							logger.error(HelperBase.getStackTrace(e1));

							continue;
						}
					}
				} else {
					logger.debug("There is no task defined");
				}
			} else {
				migrator.printUsage(runMode);
				System.exit(1);
			}

			long costMillis = System.currentTimeMillis() - startTimeMillis;
			HelperBase.printSplitLine(logger);

			if (resultSB.length() > 5) {
				logger.debug(resultSB.toString());
			} else {
				logger.debug("None task run success");
			}
			HelperBase.printSplitLine(logger);
			logger.debug("Informatica migrator finishes works");
			logger.debug("Total cost " + Math.round((costMillis / 1000)) + " seconds");
			System.exit(0);
		} else {
			migrator.printUsage(runMode);
			System.exit(1);
		}

		// HelperBase.printSplitLine(logger);
	}

	public void printUsage(int runMode) {
		String errorMsg = "***************** USAGE *************************\n";
		errorMsg += "runMode arguments are:\n";
		errorMsg += "0       => Generate OWB xml file\n";
		errorMsg += "1       => Only genearte PowerCenter xml file according to OWB metadata\n";
		errorMsg += "2       => Genearte and import PowerCenter xml file into Informatica Repository\n";
		errorMsg += "3       => Generate OWB and Informatica xml files and then import it to Informatica Repository\n";
		errorMsg += "4       => Generate OWB and Informatica xml files\n";
		errorMsg += "5       => Generate OWB mapping xml files with pattern\n";
		errorMsg += "6       => Generate all OWB xml files\n";
		errorMsg += "7       => Generate pattern matched OWB and Informatica mapping xml files and import them into Informatica repository\n";
		errorMsg += "8       => Generate pattern matched Informatica mapping xml files and import them into Informatica repository\n";
		errorMsg += "9       => Generate list of OWB mapping xml files\n";
		errorMsg += "10      => Generate list of Informatica mapping xml files\n";
		errorMsg += "11      => Generate Informatica mapping xml file according to input mapping name\n";
		errorMsg += "12      => Generate list of OWB mapping xml files according to the task.txt content\n";
		errorMsg += "13      => Generate list of Informatica mapping xml file according to the task.txt content\n";
		errorMsg += "\n";

		errorMsg += "Usage : \n";
		errorMsg += "For runMode " + runMode + " :\n";
		if (runMode <= 10) {
			errorMsg += "MappingMigrator <runMode>\n";
		} else if (runMode == 11) {
			errorMsg += "MappingMigrator <runMode> <mapping name>\n";
		} else if (runMode == 12 || runMode == 13) {
			errorMsg += "MappingMigrator <runMode> \n";
		} else if (runMode == -1) {
			errorMsg += "The runMode is out of support scope";
		}
		errorMsg += "********************************************************\n";
		System.out.println(errorMsg);
	}

}
