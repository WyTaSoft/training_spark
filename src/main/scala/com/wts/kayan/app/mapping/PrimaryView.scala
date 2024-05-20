package com.wts.kayan.app.mapping

/**
 * Object containing SQL queries for data transformation within the Spark application.
 * This object is designed to store SQL strings that are used for performing various operations
 * on data, such as joining and aggregating data from different views.
 * @author Mehdi TAJMOUATI
 * @note Ensure that the SparkSession is properly configured and active before invoking this class.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 * @see <a href="https://www.linkedin.com/in/mtajmouati">Mehdi TAJMOUATI's LinkedIn profile</a>
 */
object PrimaryView {

  /**
   * SQL query string to join client and order data, calculate the total amount of orders for each client,
   * and broadcast the client data to optimize the join operation.
   *
   * The query performs the following steps:
   * 1. Joins the `clients_view` and `orders_view` on `clientId`.
   * 2. Uses a window function to partition data by `clientId` and calculate the total order amount for each client.
   * 3. Broadcasts the client data for an optimized join operation.
   *
   * @return SQL query string for joining client and order data and calculating the total amount by client.
   */
  val get_client_order_SqlString: String =
    """
      |SELECT /*+ BROADCAST(c) */
      |  c.clientId,
      |  c.name,
      |  c.location,
      |  o.orderId,
      |  o.amount,
      |  o.date,
      |  SUM(o.amount) OVER(PARTITION BY c.clientId) AS totalAmountByClient
      |FROM
      |  clients_view c
      |LEFT JOIN
      |  orders_view o
      |ON
      |  c.clientId = o.clientId
      |""".stripMargin

}
