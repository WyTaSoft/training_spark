package com.wts.kayan.app.utility

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType

import java.io.{BufferedReader, InputStreamReader, Reader}
import java.text.SimpleDateFormat
import java.util.Date

object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  def getMaxPartition(path: String, columnPartitioned: String = "date")(
    spark: SparkSession): String = {
    val fs = org.apache.hadoop.fs.FileSystem
      .get(spark.sparkContext.hadoopConfiguration)

    try {
      val listOfInsertDates: Array[String] = fs
        .listStatus(new Path(s"$path"))
        .filter(_.isDirectory)
        .map(_.getPath)
        .map(_.toString)
      val filterPartitionFolders =
        listOfInsertDates.filter(_.contains(s"$columnPartitioned="))
      val insertDateStr = filterPartitionFolders.map(_.split(s"$columnPartitioned=")(1))

      if (insertDateStr.length > 1) {
        val dateFormatted =
          insertDateStr.map(d => convertStringToDate(d, "yyyy-MM-dd"))
        val maxDate = dateToStrNdReformat(dateFormatted.max, "yyyy-MM-dd")
        log.info(s"\n**** max $columnPartitioned $maxDate ****\n")
        maxDate
      } else if (insertDateStr.length == 0) {
        log.info(s"\n**** there are no partition by $columnPartitioned in $path  ****\n")
        "2999-01-01"
      } else {
        log.info(s"\n**** max $columnPartitioned ${insertDateStr(0)} ****\n")
        insertDateStr(0)
      }
    } catch {
      case _: Throwable =>
        println("Fatal Exception: Check Src-View Data")
        throw new ArrayIndexOutOfBoundsException

    }
  }

  def readDataFrame(sourceName: String,
                    schema: StructType,
                    isCondition: Boolean = false,
                    condition: Column = null)
                   (implicit sparkSession: SparkSession, env: String, config: Config): DataFrame = {

    log.info(s"\n**** Reading file to create DataFrame  ****\n")

    // Génération de la condition effective

    // Création d'une condition efficace
    val effectiveCondition: Column = if (isCondition) condition else lit(true)

    var inputPath: String = ""
    var tableName = ""

    sourceName match {

      case PrimaryConstants.CLIENTS =>
        inputPath = "/project/datalake/"
        tableName = "clients"

      case PrimaryConstants.ORDERS =>
        inputPath = "/project/datalake/"
        tableName = "orders"
        val PartitionedValue = getMaxPartition(s"$inputPath${tableName.toLowerCase}/")(sparkSession)
        tableName = s"orders/date=$PartitionedValue"
    }

    log.info(s"\n Loading $sourceName from $inputPath${tableName.toLowerCase} ***\n")

    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .parquet(s"$inputPath${tableName.toLowerCase}/")
      .selectExpr(ColumnSelector.getColumnSequence(sourceName): _*)
      .where(effectiveCondition)

    dataFrame
  }


  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }


  def convertStringToDate(s: String, formatType: String): Date = {
    val format = new java.text.SimpleDateFormat(formatType)
    format.parse(s)
  }

  def dateToStrNdReformat(date: Date, format: String): String = {
    val df = new SimpleDateFormat(format)
    val strDate = df.format(date)

    strDate
  }

  def writeDataFrame(dataFrame: DataFrame,
                     mode: String,
                     numPartition: Int): Unit = {

    log.info(s"\n *** Write started (mode:$mode numPartition:$numPartition) ...***\n")

    dataFrame
      .coalesce(numPartition)
      .write
      .format("parquet")
      .partitionBy("location")
      .mode(s"$mode")
      .save("/project/datalake/clients_orders")

    log.info(s"\n *** Write Completed ...***... \n")

  }

}