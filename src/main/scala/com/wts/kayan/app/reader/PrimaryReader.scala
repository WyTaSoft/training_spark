package com.wts.kayan.app.reader

import com.typesafe.config.Config
import com.wts.kayan.app.utility.{SchemaSelector, PrimaryConstants}
import com.wts.kayan.app.utility.PrimaryUtilities.readDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

class PrimaryReader()(implicit sparkSession: SparkSession, env: String, config: Config ) extends SchemaSelector {

  private lazy val clients =
    readDataFrame(PrimaryConstants.CLIENTS, clientsSchema)

  private lazy val orders =
    readDataFrame(PrimaryConstants.ORDERS, ordersSchema)

  def getDataframe(input: String): DataFrame = {
    input.toUpperCase match {
      case "CLIENTS"    => clients
      case "ORDERS"    => orders
    }
  }

}
