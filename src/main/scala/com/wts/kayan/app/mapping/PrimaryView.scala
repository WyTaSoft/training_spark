package com.wts.kayan.app.mapping

/**
 * Provides SQL query strings for data transformations within the Spark application.
 * This object contains pre-defined SQL queries that are used across various modules
 * to perform data joining and enrichment tasks efficiently using Spark SQL.
 *
 * These queries are optimized with hints like `BROADCAST` to leverage Spark's ability to optimize
 * query execution plans based on data size and distribution.
 *
 * @author Mehdi TAJMOUATI
 * @note Queries in this object are designed for specific data models and should be modified
 *       accordingly if the underlying data structures change.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's advanced Spark SQL courses.</a>
 */
object PrimaryView {

  /**
   * SQL query string to join orders and clients data with a left join operation.
   * This query enriches order data by adding client details from a client view.
   *
   * The query leverages Spark SQL's broadcast join hint to optimize the join operation by broadcasting
   * the smaller dataset (clients) across the cluster. This reduces data shuffling and improves query performance.
   *
   * @return A SQL query string that can be executed in Spark SQL to retrieve enriched order data with client details.
   */
  val get_client_order_SqlString: String =
    """
      |  SELECT /*+ BROADCAST(c) */
      |    c.clientId,
      |    c.name,
      |    c.location,
      |    o.orderId,
      |    o.amount,
      |    o.date
      |  FROM
      |    orders_view o
      |  LEFT JOIN
      |    clients_view c
      |  ON
      |    c.clientId = o.clientId
      |""".stripMargin

}
