package com.wts.kayan.app.reader

import com.typesafe.config.Config
import com.wts.kayan.app.utility.KayanConstants
import com.wts.kayan.app.utility.KayanUtilities.readDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

class KayanReader()(implicit sparkSession: SparkSession, env: String, config: Config ) {

 // val year_curr: String = getCurrYear(config)

  private lazy val clients =
    readDataFrame(KayanConstants.CLIENTS)

  clients.show()

  private lazy val orders =
    readDataFrame(KayanConstants.ORDERS)

  orders.show()

  def getDataframe(input: String): DataFrame = {

    input.toUpperCase match {
      case "CLIENTS"    => clients
      case "ORDERS"    => orders
    }
  }

}
