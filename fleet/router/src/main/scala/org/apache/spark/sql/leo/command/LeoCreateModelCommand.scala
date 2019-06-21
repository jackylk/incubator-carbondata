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

import org.apache.spark.sql.{AnalysisException, LeoDatabase, Row, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.leo.model.rest.CreateModelRestManager
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation}
import org.apache.spark.sql.leo.{LeoQueryObject, ModelStoreManager}
import org.apache.spark.sql.types.AtomicType

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.{DataMapSchema, RelationIdentifier}
import org.apache.carbondata.core.scan.expression.{Expression => CarbonExpression}
import org.apache.carbondata.core.scan.expression.logical.AndExpression
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
    val database = LeoDatabase.convertLeoDBNameToUser(parentTable.head.database)
    query
      .setTableName(database + CarbonCommonConstants.UNDERSCORE + parentTable.head.identifier.table)
    query.setTablePath(parentTable.head.storage.locationUri.get.getPath)
    // get projection columns and filter expression from logicalPlan
    logicalPlan match {
      case Project(projects, child: Filter) =>
        val projectionColumns = new util.ArrayList[String]()
        // convert expression to sparks source filter
        val filters = child.condition.flatMap(DataSourceStrategy.translateFilter)
        val tableSchema = parentTable.head.schema
        val dataTypeMap = tableSchema.map(f => f.name -> f.dataType).toMap
        // convert to carbon filter expressions
        val filter: Option[CarbonExpression] = filters.filterNot{ ref =>
          ref.references.exists{ p =>
            !dataTypeMap(p).isInstanceOf[AtomicType]
          }
        }.flatMap { filter =>
          CarbonSparkDataSourceUtil.createCarbonFilter(tableSchema, filter)
        }.reduceOption(new AndExpression(_, _))
        query.setFilterExpression(filter.get)
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
    optionsMap.putAll(options.asJava)
    optionsMap
      .put(CarbonCommonConstants.QUERY_OBJECT, ObjectSerializationUtil.convertObjectToString(query))
    // create model schema
    val modelSchema = new DataMapSchema()
    modelSchema.setDataMapName(modelName)
    modelSchema.setCtasQuery(queryString)
    modelSchema.setProperties(optionsMap)
    // get parent table relation Identifier
    val parentIdents = parentTable.map { table =>
      val relationIdentifier = new RelationIdentifier(database, table.identifier.table, "")
      relationIdentifier.setTablePath(FileFactory.getUpdatedFilePath(table.location.toString))
      relationIdentifier
    }
    modelSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    // It starts creating the training job and generates the model in cloud.
    val jobId = CreateModelRestManager.startTrainingJobRequest(optionsMap, modelName, query)
    optionsMap.put("job_id", jobId.toString)
    try {
      // store model schema
      ModelStoreManager.getInstance().saveModelSchema(modelSchema)
    } catch {
      case e:Exception =>
        // TODO drop job.
        throw e
    }

    Seq.empty
  }
}
