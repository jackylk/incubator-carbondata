package org.apache.spark.sql

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.util.SerializableConfiguration

/**
 * util for spark in cloud module
 */
object CloudUtils {

  def sessionState(sparkSession: SparkSession): SessionState = sparkSession.sessionState

  def serialize(hadoopConf: Configuration): SerializableConfiguration = {
    new SerializableConfiguration(hadoopConf)
  }

}
