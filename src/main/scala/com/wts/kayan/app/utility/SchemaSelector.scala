package com.wts.kayan.app.utility

import org.apache.spark.sql.types._

trait SchemaSelector {

  lazy val clientsSchema: StructType = StructType(
    Array(
      StructField("clientId", IntegerType),
      StructField("name", StringType),
      StructField("location", StringType)
    )
  )

  lazy val ordersSchema: StructType = StructType(
    Array(
      StructField("orderId", IntegerType),
      StructField("clientId", IntegerType),
      StructField("amount", DoubleType),
      StructField("date", DateType)
    )
  )

}
