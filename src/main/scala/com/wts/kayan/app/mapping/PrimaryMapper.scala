package com.wts.kayan.app.mapping

import PrimaryView._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A class responsible for mapping and enriching DataFrames through SQL transformations.
 * This class takes predefined DataFrames of clients and orders, and applies SQL logic
 * to enrich and transform the data, making it ready for further processing or analytics.
 *
 * The class uses Spark SQL's capabilities to execute complex queries and manipulations
 * by leveraging temporary views created from the input DataFrames.
 *
 * @param clients DataFrame containing client data.
 * @param orders DataFrame containing order data.
 * @param sparkSession Implicit SparkSession which provides the necessary context for DataFrame operations.
 * @author Mehdi TAJMOUATI
 * @note Ensures that all transformations are performed within Spark's optimized SQL engine, providing efficient data handling.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on data processing courses.</a>
 */
class PrimaryMapper(clients: DataFrame,
                    orders: DataFrame)(implicit sparkSession: SparkSession) {

  /**
   * Enriches the input DataFrames by applying a SQL transformation.
   * This method creates temporary views for the clients and orders DataFrames and then
   * executes a SQL query to join and enrich the data based on business logic defined in `PrimaryView.get_client_order_SqlString`.
   *
   * The resulting DataFrame combines and enhances the data from both views, which can then be used
   * for advanced analytics or reporting.
   *
   * @return DataFrame resulting from the SQL query, containing enriched data.
   */
  def enrichDataFrame: DataFrame = {
    // Registering DataFrames as temporary views in the Spark SQL catalog
    clients.createOrReplaceTempView("clients_view")
    orders.createOrReplaceTempView("orders_view")

    // Executing a predefined SQL query to join and enrich the data
    sparkSession.sql(PrimaryView.get_client_order_SqlString)
  }
}
