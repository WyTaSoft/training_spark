package com.wts.kayan.app.reader

import com.typesafe.config.Config
import com.wts.kayan.app.utility.PrimaryConstants
import com.wts.kayan.app.utility.PrimaryUtilities.readDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryReader()(implicit sparkSession: SparkSession, env: String, config: Config ) {

  private lazy val clients =
    readDataFrame(PrimaryConstants.CLIENTS)

  private lazy val orders =
    readDataFrame(PrimaryConstants.ORDERS)

  def getDataframe(input: String): DataFrame = {
    input.toUpperCase match {
      case "CLIENTS"    => clients
      case "ORDERS"    => orders
    }
  }

}
