package com.wts.kayan.app.utility

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

import java.io.{BufferedReader, InputStreamReader, Reader}

object KayanUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)


  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }


  def readDataFrame(table: String
                   )
                   (implicit sparkSession: SparkSession, env: String, config: Config): DataFrame = {

    log.info(s"\n**** Reading file to create DataFrame  ****\n")

    var staticInputPath: String = ""
    var tableName = ""

    table match {

      case KayanConstants.CLIENTS =>
        staticInputPath = config
          .getString(
            "training_spark.wytasoft." + KayanConstants.CLIENTS.toLowerCase + "." + env.toLowerCase
          )
        tableName = config
          .getString(
            "training_spark.wytasoft.tablename." + KayanConstants.CLIENTS.toLowerCase
          )

      case KayanConstants.ORDERS =>
        staticInputPath = config
          .getString(
            "training_spark.wytasoft." + KayanConstants.ORDERS.toLowerCase + "." + env.toLowerCase
          )
        tableName = config
          .getString(
            "training_spark.wytasoft." + KayanConstants.ORDERS.toLowerCase
          )
    }

    log.info(s"\n Loading $table from $staticInputPath${tableName.toLowerCase} ***\n")

    val dataFrame: DataFrame = sparkSession.read
      .parquet(s"$staticInputPath${tableName.toLowerCase}/")

    dataFrame
  }

}
