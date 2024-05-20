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

/**
 * Utility object providing various methods for handling data operations in a Spark application.
 * Includes methods for reading data frames, handling partitions, and reading files from HDFS.
 * These utilities are designed to facilitate common tasks and improve code reusability.
 * @author Mehdi TAJMOUATI
 * @note Ensure that the SparkSession is properly configured and active before invoking this class.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more information on Spark applications and data processing.</a>
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Retrieves the most recent partition based on a specified column from a given HDFS path.
   * This is particularly useful for incremental data loading scenarios.
   *
   * @param path The HDFS directory path to scan for partitions.
   * @param columnPartitioned The column by which the data is partitioned, default is "date".
   * @param spark Implicit SparkSession needed for accessing HDFS.
   * @return The most recent partition value as a string in "yyyy-MM-dd" format or a default future date if no partitions are found.
   * @throws ArrayIndexOutOfBoundsException If an error occurs while reading the partition data.
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

  /**
   * Reads a DataFrame from a specified source path using a predefined schema and an optional filter condition.
   *
   * @param sourceName The name of the data source to read.
   * @param schema The schema to apply to the DataFrame.
   * @param isCondition A boolean indicating whether a condition should be applied.
   * @param condition The condition to apply as a filter, defaults to no condition.
   * @param sparkSession Implicit SparkSession for DataFrame operations.
   * @param env Implicit environment string for path construction.
   * @param config Implicit Config for additional settings.
   * @return DataFrame loaded from the specified HDFS path with the applied schema and condition.
   */
  def readDataFrame(sourceName: String,
                    schema: StructType,
                    isCondition: Boolean = false,
                    condition: Column = null)
                   (implicit sparkSession: SparkSession, env: String, config: Config): DataFrame = {

    log.info(s"\n**** Reading file to create DataFrame  ****\n")

    // Génération de la condition effective
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

  /**
   * Opens a file from HDFS and returns a BufferedReader to read the file content.
   *
   * @param filePath The path to the file on HDFS.
   * @param sc SparkContext to access Hadoop configuration.
   * @return BufferedReader to read the file content.
   */
  def getHdfsReader(filePath: String)(sc: SparkContext): Reader = {
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val path = new Path(filePath)
    new BufferedReader(new InputStreamReader(fs.open(path)))
  }

  /**
   * Converts a string to a Date object based on the provided date format.
   *
   * @param s The date string to convert.
   * @param formatType The format of the date string.
   * @return Date object representing the parsed date string.
   */
  def convertStringToDate(s: String, formatType: String): Date = {
    val format = new SimpleDateFormat(formatType)
    format.parse(s)
  }

  /**
   * Reformats a Date object to a string based on the provided date format.
   *
   * @param date The Date object to reformat.
   * @param format The desired format of the date string.
   * @return String representing the formatted date.
   */
  def dateToStrNdReformat(date: Date, format: String): String = {
    val df = new SimpleDateFormat(format)
    val strDate = df.format(date)
    strDate
  }
}
