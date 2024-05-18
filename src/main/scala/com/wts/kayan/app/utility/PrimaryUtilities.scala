package com.wts.kayan.app.utility

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType

import java.io.{BufferedReader, InputStreamReader, Reader}
import java.text.SimpleDateFormat
import java.util.Date

/**
 * Utility object containing methods for interacting with HDFS, reading data frames,
 * and handling date conversions, tailored for Spark applications.
 * These utilities are designed to facilitate common tasks like reading data,
 * finding the most recent data partitions, and parsing dates, thereby simplifying data management tasks.
 *
 * @note Use these utilities to enhance code reusability and maintain clean, efficient operations within Spark jobs.
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Fetches the most recent partition based on a date column from a specified HDFS path.
   * This method is useful for incremental data loading scenarios.
   *
   * @param path The HDFS directory to scan.
   * @param columnPartitioned The partition column, defaulted to 'date'.
   * @param spark The Spark session.
   * @return The most recent partition date as a string, or a far-future date if no partitions exist.
   */
  def getMaxPartition(path: String, columnPartitioned: String = "date")(
    spark: SparkSession): String = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

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
        log.info(s"\n**** there are no partition by $columnPartitioned in $path ****\n")
        "2999-01-01"
      } else {
        log.info(s"\n**** max $columnPartitioned ${insertDateStr(0)} ****\n")
        insertDateStr(0)
      }
    } catch {
      case _: Throwable =>
        log.error("Fatal Exception: Check Src-View Data")
        throw new ArrayIndexOutOfBoundsException
    }
  }

  /**
   * Reads a DataFrame from a specified source path using a predefined schema, supporting data partitioning.
   *
   * @param sourceName The identifier for the data source to load.
   * @param schema The schema to apply to the DataFrame.
   * @param sparkSession Implicit SparkSession to handle DataFrame operations.
   * @param env Implicit environment used for building the data path.
   * @param config Implicit Config object for additional settings.
   * @return DataFrame loaded from the specified path.
   */
  def readDataFrame(sourceName: String,
                    schema: StructType)
                   (implicit sparkSession: SparkSession, env: String, config: Config): DataFrame = {
    log.info(s"\n**** Reading file to create DataFrame ****\n")
    var inputPath: String = env + "/project/datalake/"
    var tableName = sourceName

    val PartitionedValue = getMaxPartition(s"$inputPath${tableName.toLowerCase}/")(sparkSession)
    tableName = s"$sourceName/date=$PartitionedValue"

    log.info(s"\n Loading $sourceName from $inputPath${tableName.toLowerCase} ***\n")
    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .parquet(s"$inputPath${tableName.toLowerCase}/")
      .selectExpr(ColumnSelector.getColumnSequence(sourceName): _*)

    dataFrame
  }

  /**
   * Opens a file in HDFS and returns a BufferedReader to read the file's contents.
   *
   * @param filePath The full path to the file in HDFS.
   * @param sc The SparkContext to access Hadoop configurations.
   * @return A BufferedReader that can be used to read the file.
   */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  private def convertStringToDate(s: String, formatType: String): Date = {
    val format = new SimpleDateFormat(formatType)
    format.parse(s)
  }

  private def dateToStrNdReformat(date: Date, format: String): String = {
    val df = new SimpleDateFormat(format)
    val strDate = df.format(date)
    strDate
  }
}
