/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.leo

import org.apache.carbondata.spark.util.CarbonScalaUtil
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.leo.command._
import org.apache.spark.sql.util.CarbonException

import scala.util.matching.Regex

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

class LeoConsumerSqlParser extends LeoAiSqlParser {

  protected val CONSUMER: Regex = leoKeyWord("CONSUMER")
  protected val TOPIC: Regex = leoKeyWord("TOPIC")
  protected val CONSUMERS: Regex = leoKeyWord("CONSUMERS")
  protected val LAKE: Regex = leoKeyWord("LAKE")

  /**
   * This will convert key word to regular expression.
   */
  private def leoKeyWord(keys: String): Regex = {
    ("(?i)" + keys).r
  }

  override def parse(input: String): LogicalPlan = {
    synchronized {
      // Initialize the Keywords.
      initLexical
      phrase(start)(new lexical.Scanner(input)) match {
        case Success(plan, _) =>
          CarbonScalaUtil.cleanParserThreadLocals()
          plan match {
            case x: CarbonLoadDataCommand =>
              x.inputSqlString = input
              x
            case x: CarbonAlterTableCompactionCommand =>
              x.alterTableModel.alterSql = input
              x
            case logicalPlan => logicalPlan
          }
        case failure: Failure =>
          try {
            super.parse(input)
          } catch {
            case mce: MalformedCarbonCommandException => throw mce
            case e: Throwable => CarbonException.analysisException(e.getMessage)
          }
      }
    }
  }

  override protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand |
    consumerManagement

  protected lazy val consumerManagement: Parser[LogicalPlan] = createConsumer | dropConsumer |
    descConsumer | showConsumers

  /**
   * create consumer for load streaming data to hbase
   * CREATE CONSUMER c
   * ON TOPIC t1
   * INTO TABLE t
   * OPTIONS (interval=10s) AS
   * SELECT *
   * FROM t1
   * WHERE age > 10;
   */
  protected lazy val createConsumer: Parser[LogicalPlan] =
    CREATE ~> CONSUMER ~>  opt(IF ~> NOT ~> EXISTS) ~ ident ~
      (ON ~> TOPIC ~> ident) ~
      (INTO ~> TABLE ~> (ident <~ ".").?) ~ ident ~
      (OPTIONS ~> "(" ~> repsep(loadOptions, ",") <~ ")").? ~
      (AS ~> restInput) <~ opt(";") ^^ {
      case ifNotExists ~ consumerName ~ topicName ~ dbName ~ tableName ~ options ~ query =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        LeoCreateConsumerCommand(
          consumerName, topicName, dbName, tableName, ifNotExists.isDefined, optionMap, query)
    }

  /**
   * The syntax of DROP CONSUMER
   * DROP CONSUMER [IF EXISTS] consumerName ON TOPIC topicName
   */
  protected lazy val dropConsumer: Parser[LogicalPlan] =
    DROP ~> CONSUMER ~> opt(IF ~> EXISTS) ~ ident ~
      (ON ~> TOPIC ~> ident) <~ opt(";") ^^ {
      case ifExists ~ consumerName ~ topicName =>
        LeoDropConsumerCommand(consumerName, ifExists.isDefined, topicName)
    }


  /**
   * The syntax of SHOW CONSUMERS
   * SHOW CONSUMERS ON LAKE lakeName
   */
  protected lazy val showConsumers: Parser[LogicalPlan] =
    SHOW ~> CONSUMERS ~> ON ~> LAKE ~> ident <~ opt(";") ^^ {
      case lakeName =>
        LeoShowConsumersCommand(lakeName)
    }

  /**
   * The syntax of DESC CONSUMER
   * DESC CONSUMER name
   */
  protected lazy val descConsumer: Parser[LogicalPlan] =
    SHOW ~> CONSUMER ~> ident <~ opt(";") ^^ {
      case consumerName =>
        LeoDescConsumerCommand(consumerName)
    }
}
