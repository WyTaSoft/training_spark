package com.wts.kayan.app.job

import com.wts.kayan.app.utility.KayanUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.wts.kayan.SessionManager.SparkSessionManager
import com.wts.kayan.app.reader.KayanReader
import com.wts.kayan.app.utility.KayanConstants
import org.slf4j.LoggerFactory

object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param args jar parameters
    */
  def main(args: Array[String]): Unit = {

    val env = args(0)
    val absoluteConfigPath = args(1)

    val sparkSession = SparkSessionManager.fetchSparkSession(KayanConstants.APPLICATION_NAME)

    sparkSession.conf.set("spark.network.timeout", "36000")
    sparkSession.conf.set("spark.sql.broadcastTimeout", "36000")

    val reader = getHdfsReader(absoluteConfigPath)(sparkSession.sparkContext)
    val config = ConfigFactory.parseReader(reader)

    logger.info(s"\n\n****  X job has started ... **** \n\n", this.getClass.getName)

    val kayanreader = new KayanReader()(sparkSession, env, config)

    sparkSession.close()
  }
}
