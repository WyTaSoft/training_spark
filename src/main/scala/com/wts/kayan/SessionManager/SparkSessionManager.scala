package com.wts.kayan.SessionManager

import org.apache.spark.sql.SparkSession

/**
  * Singleton for Spark Session
  */
object SparkSessionManager {

  /**
    * @param appName
    * @return
    */
  def fetchSparkSession(appName: String): SparkSession = {

    SparkSession
      .builder()
      .appName(appName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.tungsten.enabled", "true")
      .config("spark.rdd.compress", "true")
      .config("spark.io.compression.codec", "snappy")
      .config("spark.sql.broadcastTimeout", 1200)
      .config("spark.eventLog.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()
  }

}
