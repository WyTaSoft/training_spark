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
 * Utility object that provides a set of functions to manage data operations and interactions with HDFS
 * within a Spark application.
 *
 * This object encapsulates methods for reading data, managing partitions, and reading files from HDFS,
 * which are common tasks in many Spark-driven data processing tasks.
 *
 * @author Mehdi TAJMOUATI
 * @note This utility module is essential for handling file operations and Spark DataFrame manipulations in a robust manner.
 * @see <a href="https://www.wytasoft.com/wytasoft-group/">Visit WyTaSoft for more on Mehdi's courses and training sessions.</a>
 */
object PrimaryUtilities {

  private val log = LoggerFactory.getLogger(this.getClass)

  /**
   * Retrieves the latest partition based on a specified column from a given HDFS path.
   *
   * @param path The HDFS directory path to scan for partitions.
   * @param columnPartitioned The column by which the data is partitioned, default is "date".
   * @param spark SparkSession object to access Spark features.
   * @return The maximum partition value as a string in "yyyy-MM-dd" format or a default future date if no partitions are found.
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
   * Reads data from HDFS and creates a DataFrame based on the specified schema and source name.
   *
   * @param sourceName The name of the data source which matches table names in 'PrimaryConstants'.
   * @param schema The schema to apply to the DataFrame.
   * @param sparkSession Implicit SparkSession for DataFrame operations.
   * @param env Implicit environment string for path construction.
   * @param config Implicit Config for additional settings.
   * @return DataFrame loaded from the specified HDFS path.
   */
  def readDataFrame(sourceName: String,
                    schema: StructType)
                   (implicit sparkSession: SparkSession, env: String, config: Config): DataFrame = {
    log.info(s"\n**** Reading file to create DataFrame  ****\n")
    var inputPath: String = ""
    var tableName = ""

    sourceName match {
      case PrimaryConstants.CLIENTS =>
        inputPath = s"$env/project/datalake/"
        tableName = "clients"

      case PrimaryConstants.ORDERS =>
        inputPath = s"$env/project/datalake/"
        tableName = "orders"
        val PartitionedValue = getMaxPartition(s"$inputPath${tableName.toLowerCase}/")(sparkSession)
        tableName = s"orders/date=$PartitionedValue"
    }

    log.info(s"\n Loading $sourceName from $inputPath${tableName.toLowerCase} ***\n")
    val dataFrame: DataFrame = sparkSession.read
      .schema(schema)
      .parquet(s"$inputPath${tableName.toLowerCase}/")
      .selectExpr(ColumnSelector.getColumnSequence(sourceName): _*)

    dataFrame
  }

  /**
   * Opens a file from HDFS and returns a BufferedReader.
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
