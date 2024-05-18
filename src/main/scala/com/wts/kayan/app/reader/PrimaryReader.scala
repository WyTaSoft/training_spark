package com.wts.kayan.app.reader

import com.typesafe.config.Config
import com.wts.kayan.app.utility.{SchemaSelector, PrimaryConstants}
import com.wts.kayan.app.utility.PrimaryUtilities.readDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Class responsible for reading and managing data from specific data sources defined in PrimaryConstants.
 * This reader utilizes predefined schemas and utility methods to efficiently load data into DataFrames.
 *
 * This class extends `SchemaSelector` to use the predefined schemas for clients and orders.
 * It simplifies data access patterns within the Spark application.
 *
 * Usage:
 * - `clients`: DataFrame loaded with the clients schema.
 * - `orders`: DataFrame loaded with the orders schema.
 *
 * @param sparkSession Implicit SparkSession provides the context for DataFrame operations.
 * @param env Implicit environment string indicating the runtime environment, used to construct paths.
 * @param config Implicit Config providing configuration settings necessary for data operations.
 * @author Mehdi TAJMOUATI
 * @note Utilizes `PrimaryUtilities.readDataFrame` for data extraction to ensure consistency and reuse of logic.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's courses and training sessions.</a>
 */
class PrimaryReader()(implicit sparkSession: SparkSession, env: String, config: Config ) extends SchemaSelector {

  /**
   * Lazy val for clients DataFrame.
   * Loads data for clients using the defined `clientsSchema` from `SchemaSelector`.
   * Data is fetched according to the paths and configuration provided by the application environment settings.
   */
  private lazy val clients: DataFrame =
    readDataFrame(PrimaryConstants.CLIENTS, clientsSchema)

  /**
   * Lazy val for orders DataFrame.
   * Loads data for orders using the defined `ordersSchema` from `SchemaSelector`.
   * Utilizes the latest partition data available, ensuring the most up-to-date data is loaded.
   */
  private lazy val orders: DataFrame =
    readDataFrame(PrimaryConstants.ORDERS, ordersSchema)
}
