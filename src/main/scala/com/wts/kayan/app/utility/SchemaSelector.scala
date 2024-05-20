package com.wts.kayan.app.utility

import org.apache.spark.sql.types._

/**
 * A trait for defining and managing schemas for different data entities within the application.
 * This ensures that all data manipulations use consistent and structured data types across the application.
 *
 * Provides pre-defined schemas for entities like clients and orders,
 * which can be reused throughout the Spark application to maintain uniformity in data handling.
 *
 * ==Overview==
 * - '''clientsSchema''': Defines schema for client data including fields like clientId, name, and location.
 * - '''ordersSchema''': Defines schema for order data including fields like orderId, clientId, amount, and date.
 *
 * @note This schema definition module is designed and maintained by Mehdi TAJMOUATI, a Big Data/Cloud Trainer.
 *       For queries, training or further information, contact: mehdi.tajmouati@wytasoft.com
 *
 * @author Mehdi TAJMOUATI
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">More information on Mehdi's training sessions and courses.</a>
 */
trait SchemaSelector {

  /** Lazy val for client schema definition.
   * Includes the following fields:
   * - clientId (IntegerType): Unique identifier for a client.
   * - name (StringType): Name of the client.
   * - location (StringType): Physical or geographical location of the client.
   */
  lazy val clientsSchema: StructType = StructType(
    Array(
      StructField("clientId", IntegerType),
      StructField("name", StringType),
      StructField("location", StringType)
    )
  )

  /** Lazy val for order schema definition.
   * Includes the following fields:
   * - orderId (IntegerType): Unique identifier for an order.
   * - clientId (IntegerType): Client identifier that links an order to a specific client.
   * - amount (DoubleType): Total monetary value of the order.
   * - date (DateType): The date on which the order was placed.
   */
  lazy val ordersSchema: StructType = StructType(
    Array(
      StructField("orderId", IntegerType),
      StructField("clientId", IntegerType),
      StructField("amount", DoubleType),
      StructField("date", DateType)
    )
  )

}