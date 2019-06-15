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

import org.apache.spark.sql.catalyst.parser.{AbstractSqlParser, AstBuilder, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.{SQLConf, VariableSubstitution}
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.sql.{CarbonSession, SparkSession}
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CarbonScalaUtil

class LeoSqlParser(conf: SQLConf, sparkSession: SparkSession) extends AbstractSqlParser {

  private val parser: LeoAiSqlParser = new LeoAiSqlParser
  override val astBuilder = CarbonReflectionUtils.getAstBuilder(conf, parser, sparkSession)
  private val substitutor = new VariableSubstitution(conf)

  override def parsePlan(sqlText: String): LogicalPlan = {
    CarbonSession.updateSessionInfoToCurrentThread(sparkSession)
    try {
      val parsedPlan = super.parsePlan(sqlText)
      CarbonScalaUtil.cleanParserThreadLocals()
      parsedPlan
    } catch {
      case ce: MalformedCarbonCommandException =>
        CarbonScalaUtil.cleanParserThreadLocals()
        throw ce
      case ex: Throwable =>
        try {
          parser.parse(sqlText)
        } catch {
          case mce: MalformedCarbonCommandException =>
            throw mce
          case e: Throwable =>
            CarbonException.analysisException(
              s"""== Parse1 ==
                 |${ex.getMessage}
                 |== Parse2 ==
                 |${e.getMessage}
               """.stripMargin.trim)
        }
    }
  }

  protected override def parse[T](command: String)(toResult: SqlBaseParser => T): T = {
    super.parse(substitutor.substitute(command))(toResult)
  }

}