package org.apache.spark.sql.leo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder}
import org.apache.spark.sql.internal.SQLConf

class LeoAiSqlParser(conf: SQLConf, sparkSession: SparkSession) extends AbstractSqlParser {
  override protected def astBuilder: AstBuilder = ???
}