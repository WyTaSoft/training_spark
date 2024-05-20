package com.wts.kayan.app.common

import com.wts.kayan.app.mapping.PrimaryMapper
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Class responsible for orchestrating the data processing pipeline in a Spark application.
 * It utilizes instances of `PrimaryReader` for data retrieval and `PrimaryMapper` for data transformation.
 * This runner triggers the entire process that fetches, maps, and enriches datasets using predefined logic.
 *
 * This class acts as a central control point for initiating complex data processing tasks,
 * simplifying the execution of multiple steps into a single callable method.
 *
 * @param fetchSource An instance of `PrimaryReader` to access methods for fetching DataFrames based on predefined schemas.
 * @param sparkSession Implicit SparkSession to provide the required context for DataFrame operations.
 * @author Mehdi TAJMOUATI
 * @note Ensure that the SparkSession is properly configured and active before invoking this class.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 */
class PrimaryRunner(fetchSource: PrimaryReader)(implicit sparkSession: SparkSession) {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Executes the data processing tasks by coordinating the fetching of source data through `PrimaryReader`
   * and applying transformations via `PrimaryMapper`.
   *
   * This method fetches client and order data, then processes it using business logic defined in `PrimaryMapper`
   * to produce an enriched DataFrame that combines elements of both datasets.
   *
   * @return DataFrame containing enriched data as a result of applying transformations on source data.
   */
  def runPrimaryRunner(): DataFrame = {
    log.info("Starting data processing...")

    // Instantiate a PrimaryMapper with the required DataFrames.
    val primaryMapper = new PrimaryMapper(
      fetchSource.getDataframe(PrimaryConstants.CLIENTS),
      fetchSource.getDataframe(PrimaryConstants.ORDERS)
    )

    // Enrich and map data using the PrimaryMapper.
    val df = primaryMapper.enrichDataFrame

    log.info("Data processing completed successfully.")
    df
  }

}