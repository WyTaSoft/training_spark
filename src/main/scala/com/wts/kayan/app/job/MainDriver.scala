package com.wts.kayan.app.job

import com.wts.kayan.app.utility.PrimaryUtilities.getHdfsReader
import com.typesafe.config.ConfigFactory
import com.wts.kayan.SessionManager.SparkSessionManager
import com.wts.kayan.app.common.PrimaryRunner
import com.wts.kayan.app.reader.PrimaryReader
import com.wts.kayan.app.utility.PrimaryConstants
import com.wts.kayan.app.writer.PrimaryWriter
import org.slf4j.LoggerFactory

object MainDriver {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    *
    * @param args jar parameters
    */
  def main(args: Array[String]): Unit = {

    val env = args(0)

    val sparkSession = SparkSessionManager.fetchSparkSession(PrimaryConstants.APPLICATION_NAME)


    logger.info(s"\n\n****  training job has started ... **** \n\n", this.getClass.getName)

    val primaryReader = new PrimaryReader()(sparkSession, env)
    val primaryWriter = new PrimaryWriter()(env)
    val primaryRunner = new PrimaryRunner(primaryReader)(sparkSession).runPrimaryRunner()

    primaryRunner.show()

    primaryWriter.write(primaryRunner, PrimaryConstants.MODE_APPEND, 150)(env)


    sparkSession.close()
  }
}
