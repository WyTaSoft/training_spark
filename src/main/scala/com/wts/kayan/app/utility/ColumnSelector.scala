package com.wts.kayan.app.utility

object ColumnSelector {

  /**
    * {TABLES} Columns selected for respective tables
    */
  def getColumnSequence(tableName: String): Array[String] = {

    tableName.toLowerCase match {

      case PrimaryConstants.ORDERS =>
        Array(
          "clientid",
          "name",
          "location"
        )

      case PrimaryConstants.CLIENTS =>
        Array(
          "orderid",
          "clientid",
          "amount",
          "date"
        )
    }
  }
}
