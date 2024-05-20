package com.wts.kayan.app.writer

import com.wts.kayan.app.utility.PrimaryUtilities
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * A class responsible for writing DataFrames to storage using specified parameters.
 * This class leverages utility methods from `PrimaryUtilities` to perform the write operation.
 *
 * @constructor Create a new PrimaryWriter with an implicit environment string.
 * @param env The environment string (e.g., 'dev', 'test', 'prod').
 * @note Ensure that the SparkSession is properly configured and active before using this class.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 * @see <a href="https://www.linkedin.com/in/mtajmouati">Mehdi TAJMOUATI's LinkedIn profile</a>
 */
class PrimaryWriter()(implicit env: String) {

  private val log = LoggerFactory.getLogger(this.getClass.getName)

  /**
   * Write a DataFrame to storage with the specified mode and number of partitions.
   * This method uses `PrimaryUtilities.writeDataFrame` to perform the actual write operation.
   *
   * @param dataFrame The DataFrame to write to storage.
   * @param mode The write mode (e.g., "overwrite", "append").
   * @param numPartition The number of partitions to use when writing the DataFrame.
   * @param env Implicit environment string for the write operation.
   */
  def write(dataFrame: DataFrame,
            mode: String,
            numPartition: Int)(implicit env: String): Unit = {

    PrimaryUtilities.writeDataFrame(dataFrame, mode, numPartition)(env)

  }

}
