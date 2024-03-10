package com.wts.kayan.app.writer

import com.wts.kayan.app.utility.PrimaryUtilities
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryWriter() {

  private val log = LoggerFactory.getLogger(this.getClass.getName)

  def write(dataFrame: DataFrame,
            mode: String,
            numPartition:Int)(implicit env: String): Unit = {

    PrimaryUtilities.writeDataFrame(dataFrame,mode,numPartition)

  }

}
