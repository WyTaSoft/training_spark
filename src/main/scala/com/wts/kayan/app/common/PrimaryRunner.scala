package com.wts.kayan.app.common

import com.wts.kayan.app.mapping.PrimaryMapper
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class PrimaryRunner(fetchSource: PrimaryReader)(implicit sparkSession: SparkSession)
{

  private val log = LoggerFactory.getLogger(this.getClass)

  def runPrimaryRunner(): DataFrame = {

    println(
      s"\n\n**** Temp view creation ...****\n\n",
      this.getClass.getName
    )
    log.info(
      s"\n\n**** Temp view creation ...****\n\n",
      this.getClass.getName
    )

    val primaryMapper =
      new PrimaryMapper(
        fetchSource.getDataframe(PrimaryConstants.CLIENTS),
        fetchSource.getDataframe(PrimaryConstants.ORDERS)
      )

    val df =  primaryMapper.enrichDataFrame

    df
  }

}
