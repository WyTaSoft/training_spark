package com.wts.kayan.app.job

import com.wts.kayan.app.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.wts.kayan.SessionManager.SparkSessionManager
import com.wts.kayan.app.common.PrimaryRunner
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import org.slf4j.LoggerFactory

/**
 * The main driver for the Spark application, responsible for orchestrating the initialization,
 * configuration, and execution of the data processing workflow.
 *
 * This object sets up the Spark session, configures the application using external settings,
 * and manages the execution of data processing tasks through the `PrimaryRunner`.
 *
 * @note This is the entry point of the application, intended to be packaged as a JAR and run with Spark.
 *       It expects command-line arguments to specify environment settings and the path to the configuration file.
 */
object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main method that serves as the entry point for the Spark application.
   * Initializes necessary components and starts the data processing pipeline.
   *
   * @param args Command-line arguments received from the user, expected to include:
   *             - args(0): Environment identifier (e.g., 'dev', 'test', 'prod')
   *             - args(1): Absolute path to the configuration file in HDFS
   */
  def main(args: Array[String]): Unit = {
    // Extracting environment settings and configuration path from command-line arguments
    val env = args(0)
    val absoluteConfigPath = args(1)

    // Establishing a Spark session for the application
    val sparkSession = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    // Reading configuration from the specified HDFS path
    val reader = getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    val config = ConfigFactory.parseReader(reader)

    // Logging the start of the application
    logger.info(s"\n\n****  training job has started ... **** \n\n")

    // Initializing data reader and runner components
    val primaryReader = new PrimaryReader()(sparkSession, env, config)
    val primaryRunner = new PrimaryRunner(primaryReader)(sparkSession)

    // Executing the data processing tasks
    primaryRunner.runPrimaryRunner()

    // Closing the Spark session after execution completes
    sparkSession.close()
  }
}
