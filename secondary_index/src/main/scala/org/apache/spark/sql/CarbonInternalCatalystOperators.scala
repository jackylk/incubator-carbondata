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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.StringType

/**
 * Shows Indexes in a table
 */
case class ShowIndexesCommand(databaseNameOp: Option[String],
    table: String,
    var showIndexSql: String = null)
  extends Command {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }
}

/**
 * Drop index in a table
 */
case class DropIndexCommand(ifExistsSet: Boolean, databaseNameOp: Option[String],
    tableName: String, parentTableName: String, var dropIndexSql: String = null)
  extends Command {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("result", StringType, nullable = false)())
  }
}
