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
 * @param args Command-line arguments received from the user, expected to include:
 *             - args(0): Environment identifier (e.g., 'dev', 'test', 'prod')
 *             - args(1): Absolute path to the configuration file in HDFS
 * @note Ensure that the SparkSession is properly configured and active before invoking this class.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 * @see <a href="https://www.linkedin.com/in/mtajmouati">Mehdi TAJMOUATI's LinkedIn profile</a>
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

    val env = args(0)
    val absoluteConfigPath = args(1)

    val sparkSession = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    val reader = getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    val config = ConfigFactory.parseReader(reader)

    logger.info(s"\n\n****  training job has started ... **** \n\n", this.getClass.getName)

    val primaryReader = new PrimaryReader()(sparkSession, env, config)

    val primaryRunner = new PrimaryRunner(primaryReader)(sparkSession).runPrimaryRunner()

    sparkSession.close()
  }
}
