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
  protected val MODELS: Regex = leoKeyWord("MODELS")
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

  protected lazy val modelManagement: Parser[LogicalPlan] = createModel | dropModel | showModels
  protected lazy val serviceManagement: Parser[LogicalPlan] = registerModel | unregisterModel

  /**
   * CREATE MODEL [IF NOT EXISTS] [dbName.]modelName
   * OPTIONS (...)
   * AS select_query
   */
  protected lazy val createModel: Parser[LogicalPlan] =
    CREATE ~> MODEL ~> opt(IF ~> NOT ~> EXISTS) ~ (ident <~ ".").? ~ ident ~
    (OPTIONS ~> "(" ~> repsep(createModelOptions, ",") <~ ")").? ~
    (AS ~> restInput) <~ opt(";") ^^ {
      case ifNotExists ~ dbName ~ modelName ~ options ~ query =>
        val optionMap = options.getOrElse(List[(String, String)]()).toMap[String, String]
        LeoCreateModelCommand(dbName, modelName, optionMap, ifNotExists.isDefined, query)
    }

  protected lazy val createModelOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  /**
   * DROP MODEL [IF EXISTS] [dbName.]modelName
   */
  protected lazy val dropModel: Parser[LogicalPlan] =
    DROP ~> MODEL ~> opt(IF ~> EXISTS) ~ (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case ifExists ~ dbName ~ modelName =>
        LeoDropModelCommand(dbName, modelName, ifExists.isDefined)
    }

  /**
   * SHOW MODELS
   */
  protected lazy val showModels: Parser[LogicalPlan] =
    SHOW ~> MODELS <~ opt(";") ^^ {
      case _ =>
        LeoShowModelsCommand()
    }

  /**
   * REGISTER [dbName.]modelName AS udfName
   */
  protected lazy val registerModel: Parser[LogicalPlan] =
    REGISTER ~> MODEL ~> (ident <~ ".").? ~ ident ~ (AS ~> ident) <~ opt(";") ^^ {
      case dbName ~ modelName ~ udfName =>
        LeoRegisterModelCommand(dbName, modelName, udfName)
    }

  /**
   * UNREGISTER [dbName.]modelName
   */
  protected lazy val unregisterModel: Parser[LogicalPlan] =
    UNREGISTER ~> MODEL ~> (ident <~ ".").? ~ ident <~ opt(";") ^^ {
      case dbName ~ modelName =>
        LeoUnregisterModelCommand(dbName, modelName)
    }
}
