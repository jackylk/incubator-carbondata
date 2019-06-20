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

package org.apache.spark.sql.leo.command

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.leo.{LeoQueryObject, ModelStoreManager}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, RelationIdentifier}
import org.apache.carbondata.core.util.ObjectSerializationUtil

case class LeoCreateModelCommand(
    dbName: Option[String],
    modelName: String,
    options: Map[String, String],
    ifNotExists: Boolean,
    queryString: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // check if model with modelName already exists
    val modelSchemas = ModelStoreManager.getInstance().getAllModelSchemas
    val ifAlreadyExists = modelSchemas.asScala
      .exists(model => model.getDataMapName.equalsIgnoreCase(modelName))
    if (ifAlreadyExists) {
      if (!ifNotExists) {
        throw new AnalysisException("Model with name " + modelName + " already exists in storage")
      } else {
        return Seq.empty
      }
    }
    val dataFrame = sparkSession.sql(queryString)
    val logicalPlan = dataFrame.logicalPlan

    val parentTable = logicalPlan.collect {
      case l: LogicalRelation => l.catalogTable.get
      case h: HiveTableRelation => h.tableMeta
    }
    val query = new LeoQueryObject
    query.setTableName(parentTable.head.identifier.table)
    // get projection columns and filter expression from logicalPlan
    logicalPlan match {
      case Project(projects, child: Filter) =>
        val projectionColumns = new util.ArrayList[String]()
        query.setFilterExpression(child.condition)
        projects.map {
          case attr: AttributeReference =>
            projectionColumns.add(attr.name)
          case Alias(attr: AttributeReference, _) =>
            projectionColumns.add(attr.name)
        }
        query.setProjectionColumns(projectionColumns.asScala.toArray)
    }

    // TODO train model and get options
    val optionsMap = new java.util.HashMap[String, String]()

    optionsMap
      .put(CarbonCommonConstants.QUERY_OBJECT, ObjectSerializationUtil.convertObjectToString(query))
    // create model schema
    val modelSchema = new DataMapSchema()
    modelSchema.setDataMapName(modelName)
    modelSchema.setCtasQuery(queryString)
    modelSchema.setProperties(optionsMap)
    // get parent table relation Identifier
    val parentIdents = parentTable.map { table =>
      val relationIdentifier = new RelationIdentifier(table.database, table.identifier.table, "")
      relationIdentifier.setTablePath(FileFactory.getUpdatedFilePath(table.location.toString))
      relationIdentifier
    }
    modelSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    // store model schema
    ModelStoreManager.getInstance().saveModelSchema(modelSchema)
    Seq.empty
  }
}
