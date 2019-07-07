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

import scala.util.matching.Regex

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonLoadDataCommand}
import org.apache.spark.sql.leo.command._
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.spark.util.CarbonScalaUtil

class LeoAiSqlParser extends CarbonSpark2SqlParser {

  protected val MODEL: Regex = leoKeyWord("MODEL")
  protected val EXPERIMENT: Regex = leoKeyWord("EXPERIMENT")
  protected val REGISTER: Regex = leoKeyWord("REGISTER")
  protected val UNREGISTER: Regex = leoKeyWord("UNREGISTER")

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
        case failureOrError =>
          CarbonScalaUtil.cleanParserThreadLocals()
          CarbonException.analysisException(failureOrError.toString)
      }
    }
  }

  override protected lazy val start: Parser[LogicalPlan] = explainPlan | startCommand |
                                                           modelManagement | serviceManagement

  protected lazy val modelManagement: Parser[LogicalPlan] = createModel | dropModel |
                                                            createExperiment | dropExperiment
  protected lazy val serviceManagement: Parser[LogicalPlan] = registerModel | unregisterModel


  /**
   * CREATE EXPERIMENT [IF NOT EXISTS] experimentName
   * OPTIONS (...)
   * AS select_query
   */
  protected lazy val createExperiment: Parser[LogicalPlan] =
    CREATE ~> EXPERIMENT ~> opt(IF ~> NOT ~> EXISTS) ~  ident ~
    (OPTIONS ~> "(" ~> repsep(createModelOptions, ",") <~ ")").? ~
    (AS ~> restInput) <~ opt(";") ^^ {
      case ifNotExists ~ experimentName ~ options ~ query =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        LeoCreateExperimentCommand(experimentName, optionMap, ifNotExists.isDefined, query)
    }

  /**
   * DROP EXPERIMENT [IF EXISTS] experimentName
   */
  protected lazy val dropExperiment: Parser[LogicalPlan] =
    DROP ~> EXPERIMENT ~> opt(IF ~> EXISTS) ~ ident <~ opt(";") ^^ {
      case ifExists ~ experimentName =>
        LeoDropExperimentCommand(experimentName, ifExists.isDefined)
    }

  /**
   * CREATE MODEL [IF NOT EXISTS] modelName
   * USING EXPERIMENT experimentName
   * OPTIONS (...)
   */
  protected lazy val createModel: Parser[LogicalPlan] =
    CREATE ~> MODEL ~> opt(IF ~> NOT ~> EXISTS) ~ ident ~
    (USING ~ EXPERIMENT ~> ident) ~
    (OPTIONS ~> "(" ~> repsep(createModelOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case ifNotExists ~ modelName ~ experimentName ~ options =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        LeoCreateModelCommand(modelName, experimentName, optionMap, ifNotExists.isDefined)
    }

  protected lazy val createModelOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  /**
   * DROP MODEL [IF EXISTS] modelName on EXPERIMENT experimentName
   */
  protected lazy val dropModel: Parser[LogicalPlan] =
    DROP ~> MODEL ~> opt(IF ~> EXISTS) ~ ident ~ (ON ~ EXPERIMENT ~> ident) <~ opt(";") ^^ {
      case ifExists ~ modelName ~ experimentName =>
      LeoDropModelCommand(modelName, experimentName, ifExists.isDefined)
    }

  /**
   * REGISTER MODEL experimentName.modelName AS udfName
   */
  protected lazy val registerModel: Parser[LogicalPlan] =
    REGISTER ~> MODEL ~> (ident <~ ".") ~ ident ~ (AS ~> ident) <~ opt(";") ^^ {
      case experimentName ~ modelName ~ udfName =>
        LeoRegisterModelCommand(experimentName, modelName, udfName)
    }

  /**
   * UNREGISTER MODEL experimentName.modelName
   */
  protected lazy val unregisterModel: Parser[LogicalPlan] =
    UNREGISTER ~> MODEL ~> (ident <~ ".") ~ ident <~ opt(";") ^^ {
      case experimentName ~ modelName =>
        LeoUnregisterModelCommand(experimentName, modelName)
    }
}
