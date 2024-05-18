package com.wts.kayan.app.job

import com.wts.kayan.app.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.wts.kayan.SessionManager.SparkSessionManager
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import org.slf4j.LoggerFactory

/**
 * Main driver object for the Spark application, handling the initialization and orchestration of data processing tasks.
 *
 * This object is responsible for setting up the Spark session, reading application configuration,
 * and managing the flow of data processing through various components of the application.
 *
 * @author Mehdi TAJMOUATI
 * @note This is the entry point of the application. It is designed to handle command-line arguments to configure
 *       runtime settings and initiate the Spark processing jobs.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's courses and training sessions.</a>
 */
object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Main method to launch the application.
   * This method parses command-line arguments to set up the environment and configuration path,
   * creates a Spark session, and initiates the data processing tasks.
   *
   * @param args Command-line arguments expected to contain:
   *             0: Environment identifier (e.g., "dev", "test", "prod")
   *             1: Absolute path to the configuration file
   */
  def main(args: Array[String]): Unit = {
    // Parsing command-line arguments
    val env = args(0)
    val absoluteConfigPath = args(1)

    // Fetching the Spark session
    val sparkSession = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)

    // Reading configuration file using HDFS reader
    val reader = getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    val config = ConfigFactory.parseReader(reader)

    // Logging the start of the application
    logger.info(
      s"""
         |
         |****  training job has started ... ****
         |
         |""".stripMargin, this.getClass.getName)

    // Initializing the primary reader for data operations
    val kayanreader = new PrimaryReader()(sparkSession, env, config)

    // Closing the Spark session at the end of application run
    sparkSession.close()
  }
}
