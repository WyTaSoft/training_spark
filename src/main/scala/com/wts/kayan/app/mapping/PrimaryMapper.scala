package com.wts.kayan.app.mapping

import PrimaryView._
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryMapper(clients: DataFrame,
                    orders: DataFrame)(implicit sparkSession: SparkSession)
{

 def enrichDataFrame: DataFrame = {

    clients.createOrReplaceTempView("clients_view")

    orders.createOrReplaceTempView("orders_view")

   sparkSession.sql(get_client_order_SqlString)
  }

}
