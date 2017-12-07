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

package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.ShowIndexesCommand
import org.apache.spark.sql.command.{CreateIndexTable, DropIndex, SecondaryIndex}

import org.apache.carbondata.spark.util.CommonUtil


/**
 *
 */
class CarbonInternalSpark2SqlParser extends CarbonSpark2SqlParser {

  protected val INDEX = carbonInternalKeyWord("INDEX")
  protected val INDEXES = carbonInternalKeyWord("INDEXES")
  override protected val ON = carbonInternalKeyWord("ON")

  override def parse(input: String): LogicalPlan = {
    synchronized {
      // Initialize the Keywords.
      initLexical
      phrase(internalStartCommand)(new lexical.Scanner(input)) match {
        case Success(plan, _) => plan match {
          case x: CreateIndexTable =>
            x.inputSqlString = input
            x
          case x: ShowIndexesCommand =>
            x.showIndexSql = input
            x
          case x: DropIndex =>
            x.dropIndexSql = input
            x
          case _ => super.parse(input)
        }
        case failureOrError =>
          sys.error(failureOrError.toString)
      }
    }
  }

  /**
   * This will convert key word to regular expression.
   *
   * @param keys
   * @return
   */
  private def carbonInternalKeyWord(keys: String) = {
    ("(?i)" + keys).r
  }

  protected lazy val internalStartCommand: Parser[LogicalPlan] = indexCommands | start

  protected lazy val indexCommands: Parser[LogicalPlan] =
    showIndexes | createIndexTable | dropIndexTable

  protected lazy val createIndexTable: Parser[LogicalPlan] =
    CREATE ~> INDEX ~> ident ~ (ON ~> TABLE ~> (ident <~ ".").? ~ ident) ~
    ("(" ~> repsep(ident, ",") <~ ")") ~ (AS ~> stringLit) ~
    (TBLPROPERTIES ~> "(" ~> repsep(loadOptions, ",") <~ ")").? <~ opt(";") ^^ {
      case indexTableName ~ table ~ cols ~ indexStoreType ~ tblProp =>

        if (!("carbondata".equalsIgnoreCase(indexStoreType) ||
              "org.apache.carbondata.format".equalsIgnoreCase(indexStoreType))) {
          sys.error("Not a carbon format request")
        }

        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }

        val tableProperties = if (tblProp.isDefined) {
          val tblProps = tblProp.get.map(f => f._1 -> f._2)
          scala.collection.mutable.Map(tblProps: _*)
        } else {
          scala.collection.mutable.Map.empty[String, String]
        }
        // validate the tableBlockSize from table properties
        CommonUtil.validateTableBlockSize(tableProperties)
        val indexTableModel = SecondaryIndex(dbName,
          tableName.toLowerCase,
          cols.map(f => f.toLowerCase),
          indexTableName.toLowerCase)
        CreateIndexTable(indexTableModel, tableProperties)
    }

  protected lazy val dropIndexTable: Parser[LogicalPlan] =
    DROP ~> INDEX ~> opt(IF ~> EXISTS) ~ ident ~ (ON ~> (ident <~ ".").? ~ ident) <~ opt(";") ^^ {
      case ifexist ~ indexTableName ~ table =>
        val (dbName, tableName) = table match {
          case databaseName ~ tableName => (databaseName, tableName.toLowerCase())
        }
        DropIndex(ifexist.isDefined, dbName, indexTableName.toLowerCase, tableName)
    }

  protected lazy val showIndexes: Parser[LogicalPlan] =
    (SHOW ~> opt(FORMATTED)) ~> (INDEXES | INDEX) ~> ON ~> ident ~ opt((FROM | IN) ~> ident) <~
    opt(";") ^^ {
      case tableName ~ databaseName =>
        ShowIndexesCommand(databaseName, tableName.toLowerCase)
    }
}
