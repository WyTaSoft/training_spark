// Package declaration which organizes the code modules, similar to a folder structure.
package com.wts.kayan.SessionManager

// Importing SparkSession from Spark SQL library.
import org.apache.spark.sql.SparkSession

/**
 * Singleton object to manage Spark Session creation.
 *
 * @author Mehdi TAJMOUATI
 * @note Big Data/Cloud Trainer at WyTaSoft.
 *       For queries, training or further information, contact mehdi.tajmouati@wytasoft.com
 *       This design ensures that only one instance of SparkSession is used throughout the application.
 *       The SparkSession is configured with enhanced settings for performance and efficiency.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on courses and training sessions.</a>
 */
object SparkSessionManager {

  /**
   * Fetches or creates a SparkSession with specified application name and configuration settings.
   *
   * @param appName The name of the application. This name will be displayed in the Spark UI.
   * @return An instance of SparkSession tailored for the application, ensuring optimal performance.
   */
  def fetchSparkSession(appName: String): SparkSession = {

    // Builder pattern to construct a SparkSession with specific configurations.
    SparkSession
      .builder()
      .appName(appName)  // Sets the name for the application in the Spark UI.
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  // Optimizes object serialization for better performance.
      .config("spark.sql.tungsten.enabled", "true")  // Activates Project Tungsten for improved memory management and execution efficiency.
      .config("spark.rdd.compress", "true")  // Enables compression of RDDs, reducing memory footprint.
      .config("spark.io.compression.codec", "snappy")  // Applies Snappy compression to internal data transfers, enhancing IO efficiency.
      .config("spark.sql.broadcastTimeout", 1200)  // Extends the timeout for broadcast operations to prevent failures in larger environments.
      .config("spark.eventLog.enabled", "true")  // Enables detailed event logging, useful for debugging and performance tuning.
      .enableHiveSupport()  // Integrates Hive support, enabling SQL-like manipulation of stored data.
      .getOrCreate()  // Ensures a single instance of SparkSession per JVM, reusing the existing session or creating a new one as needed.

    // The getOrCreate method is critical for managing resource efficiency and ensuring application stability.
  }

}
