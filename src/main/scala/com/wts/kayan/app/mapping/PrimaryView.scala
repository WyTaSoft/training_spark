package com.wts.kayan.app.mapping

object PrimaryView {

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
      |JOIN
      |  orders_view o
      |ON
      |  c.clientId = o.clientId
      |""".stripMargin

}
