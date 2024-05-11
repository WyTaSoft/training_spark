package com.wts.kayan.app.mapping

object PrimaryView {

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
