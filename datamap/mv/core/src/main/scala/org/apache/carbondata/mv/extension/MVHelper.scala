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

package org.apache.carbondata.mv.extension

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Cast, Coalesce, Expression, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Average}
import org.apache.spark.sql.catalyst.plans.logical.{Join, Limit, LogicalPlan}
import org.apache.spark.sql.execution.command.{Field, PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableCommand, CarbonDropTableCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{ArrayType, DateType, MapType, StructType}
import org.apache.spark.util.{DataMapUtil, PartitionUtils}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.datamap.DataMapManager
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan}
import org.apache.carbondata.mv.plans.util.SQLBuilder
import org.apache.carbondata.mv.rewrite.{SummaryDatasetCatalog, Utils}
import org.apache.carbondata.mv.timeseries.{TimeSeriesFunction, TimeSeriesUtil}
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Utility for MV datamap operations.
 */
object MVHelper {

  def createMVDataMap(sparkSession: SparkSession,
      dataMapSchema: DataMapSchema,
      queryString: String,
      ifNotExistsSet: Boolean = false,
      mainTable: CarbonTable): Unit = {
    val dmProperties = dataMapSchema.getProperties.asScala
    if (dmProperties.contains("streaming") && dmProperties("streaming").equalsIgnoreCase("true")) {
      throw new MalformedCarbonCommandException(
        s"Materialized view does not support streaming"
      )
    }
    val mvUtil = new MVUtil
    mvUtil.validateDMProperty(dmProperties)
    val logicalPlan = Utils.dropDummyFunc(
      MVParser.getMVPlan(queryString, sparkSession))
    // if there is limit in MV ctas query string, throw exception, as its not a valid usecase
    logicalPlan match {
      case Limit(_, _) =>
        throw new MalformedCarbonCommandException("Materialized view does not support the query " +
                                                  "with limit")
      case _ =>
    }
    val selectTables = getTables(logicalPlan)
    if (selectTables.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"Non-Carbon table does not support creating MV datamap")
    }
    val modularPlan = validateMVQuery(sparkSession, logicalPlan)
    val updatedQueryWithDb = modularPlan.asCompactSQL
    val (timeSeriesColumn, granularity): (String, String) = validateMVTimeSeriesQuery(
      logicalPlan,
      dataMapSchema)
    val fullRebuild = isFullReload(logicalPlan)
    var counter = 0
    // the ctas query can have duplicate columns, so we should take distinct and create fields,
    // so that it won't fail during create mv table
    val fields = logicalPlan.output.map { attr =>
      if (attr.dataType.isInstanceOf[ArrayType] || attr.dataType.isInstanceOf[StructType] ||
          attr.dataType.isInstanceOf[MapType]) {
        throw new UnsupportedOperationException(
          s"MV datamap is not supported for complex datatype columns and complex datatype return " +
          s"types of function :" + attr.name)
      }
      val name = updateColumnName(attr, counter)
      counter += 1
      val rawSchema = '`' + name + '`' + ' ' + attr.dataType.typeName
      if (attr.dataType.typeName.startsWith("decimal")) {
        val (precision, scale) = CommonUtil.getScaleAndPrecision(attr.dataType.catalogString)
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          precision = precision,
          scale = scale,
          rawSchema = rawSchema)
      } else {
        Field(column = name,
          dataType = Some(attr.dataType.typeName),
          name = Some(name),
          children = None,
          rawSchema = rawSchema)
      }
    }.distinct

    val tableProperties = mutable.Map[String, String]()
    val parentTables = new util.ArrayList[String]()
//    val parentTablesList = new util.ArrayList[CarbonTable](selectTables.size)
//    selectTables.foreach { selectTable =>
//      val mainCarbonTable = try {
//        Some(CarbonEnv.getCarbonTable(selectTable.identifier.database,
//          selectTable.identifier.table)(sparkSession))
//      } catch {
//        // Exception handling if it's not a CarbonTable
//        case ex: Exception =>
//          throw new MalformedCarbonCommandException(
//            s"Non-Carbon table does not support creating MV datamap")
//      }
//      if (!mainCarbonTable.get.getTableInfo.isTransactionalTable) {
//        throw new MalformedCarbonCommandException("Unsupported operation on NonTransactional table")
//      }
//      if (mainCarbonTable.get.isChildTableForMV) {
//        throw new MalformedCarbonCommandException(
//          "Cannot create Datamap on child table " + mainCarbonTable.get.getTableUniqueName)
//      }
//      parentTables.add(mainCarbonTable.get.getTableName)
//      if (!mainCarbonTable.isEmpty && mainCarbonTable.get.isStreamingSink) {
//        throw new MalformedCarbonCommandException(
//          s"Streaming table does not support creating materialized view")
//      }
//      parentTablesList.add(mainCarbonTable.get)
//    }
//
//    // Check if load is in progress in any of the parent table mapped to the datamap
//    parentTablesList.asScala.foreach {
//      parentTable =>
//        if (SegmentStatusManager.isLoadInProgressInTable(parentTable)) {
//          throw new UnsupportedOperationException(
//            "Cannot create mv datamap table when insert is in progress on parent table: " +
//            parentTable.getTableName)
//        }
//    }

    tableProperties.put(CarbonCommonConstants.DATAMAP_NAME, dataMapSchema.getDataMapName)
    tableProperties.put(CarbonCommonConstants.PARENT_TABLES, parentTables.asScala.mkString(","))

    val finalModularPlan = new SQLBuilder(modularPlan).SQLizer.execute(modularPlan)
    val fieldRelationMap = mvUtil.getFieldsAndDataMapFieldsFromPlan(finalModularPlan,
      getLogicalRelation(logicalPlan))
    // If dataMap is mapped to single main table, then inherit table properties from main table,
    // else, will use default table properties. If DMProperties contains table properties, then
    // table properties of datamap table will be updated
//    if (parentTablesList.size() == 1) {
//      DataMapUtil
//        .inheritTablePropertiesFromMainTable(
//          parentTablesList.get(0),
//          fields,
//          fieldRelationMap,
//          tableProperties)
//      if (granularity != null) {
//        if (null != mainTable) {
//          if (!mainTable.getTableName.equalsIgnoreCase(parentTablesList.get(0).getTableName)) {
//            throw new MalformedCarbonCommandException(
//              "Parent table name is different in Create and Select Statement")
//          }
//        }
//        val timeSeriesDataType = parentTablesList
//          .get(0)
//          .getTableInfo
//          .getFactTable
//          .getListOfColumns
//          .asScala
//          .filter(columnSchema => columnSchema.getColumnName
//            .equalsIgnoreCase(timeSeriesColumn))
//          .head
//          .getDataType
//        if (timeSeriesDataType.equals(DataTypes.DATE) ||
//            timeSeriesDataType.equals(DataTypes.TIMESTAMP)) {
//          // if data type is of Date type, then check if given granularity is valid for date type
//          if (timeSeriesDataType.equals(DataTypes.DATE)) {
//            TimeSeriesUtil.validateTimeSeriesGranularityForDate(granularity)
//          }
//        } else {
//          throw new MalformedCarbonCommandException(
//            "TimeSeries Column must be of TimeStamp or Date type")
//        }
//      }
//    }
    dmProperties.foreach(t => tableProperties.put(t._1, t._2))
    val usePartitioning = dmProperties.getOrElse("partitioning", "true").toBoolean
    var partitionerFields: Seq[PartitionerField] = Seq.empty
//    // Inherit partition from parent table if datamap is mapped to single parent table
//    if (parentTablesList.size() == 1) {
//      val partitionInfo = parentTablesList.get(0).getPartitionInfo
//      val parentPartitionColumns = if (!usePartitioning) {
//        Seq.empty
//      } else if (parentTablesList.get(0).isHivePartitionTable) {
//        partitionInfo.getColumnSchemaList.asScala.map(_.getColumnName)
//      } else {
//        Seq()
//      }
//      partitionerFields = PartitionUtils
//        .getPartitionerFields(parentPartitionColumns, fieldRelationMap)
//    }

    var order = 0
    val columnOrderMap = new java.util.HashMap[Integer, String]()
    if (partitionerFields.nonEmpty) {
      fields.foreach { field =>
        columnOrderMap.put(order, field.column)
        order += 1
      }
    }

    // TODO Use a proper DB
    val tableIdentifier =
    TableIdentifier(dataMapSchema.getDataMapName + "_table",
      selectTables.head.identifier.database)
    // prepare table model of the collected tokens
    val tableModel: TableModel = CarbonParserUtil.prepareTableModel(
      ifNotExistPresent = ifNotExistsSet,
      CarbonParserUtil.convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      partitionerFields,
      tableProperties,
      None,
      isAlterFlow = false,
      None)

    val tablePath = if (dmProperties.contains("path")) {
      dmProperties("path")
    } else {
      CarbonEnv.getTablePath(tableModel.databaseNameOp, tableModel.tableName)(sparkSession)
    }
    CarbonCreateTableCommand(TableNewProcessor(tableModel),
      tableModel.ifNotExistsSet, Some(tablePath), isVisible = false).run(sparkSession)

    // Map list of main table columns mapped to datamap table and add to dataMapSchema
    val mainTableToColumnsMap = new java.util.HashMap[String, util.Set[String]]()
    val mainTableFieldIterator = fieldRelationMap.values.asJava.iterator()
    while (mainTableFieldIterator.hasNext) {
      val value = mainTableFieldIterator.next()
      value.columnTableRelationList.foreach {
        columnTableRelation =>
          columnTableRelation.foreach {
            mainTable =>
              if (null == mainTableToColumnsMap.get(mainTable.parentTableName)) {
                val columns = new util.HashSet[String]()
                columns.add(mainTable.parentColumnName.toLowerCase())
                mainTableToColumnsMap.put(mainTable.parentTableName, columns)
              } else {
                mainTableToColumnsMap.get(mainTable.parentTableName)
                  .add(mainTable.parentColumnName.toLowerCase())
              }
          }
      }
    }
    dataMapSchema.setMainTableColumnList(mainTableToColumnsMap)
    dataMapSchema.setColumnsOrderMap(columnOrderMap)
    if (null != granularity && null != timeSeriesColumn) {
      dataMapSchema.setCtasQuery(queryString)
      dataMapSchema.setTimeSeries(true)
    } else {
      dataMapSchema.setCtasQuery(updatedQueryWithDb)
    }
    dataMapSchema
      .setRelationIdentifier(new RelationIdentifier(tableIdentifier.database.get,
        tableIdentifier.table,
        CarbonEnv.getCarbonTable(tableIdentifier)(sparkSession).getTableId))

    val parentIdents = selectTables.map { table =>
      val relationIdentifier = new RelationIdentifier(table.database, table.identifier.table, "")
      relationIdentifier.setTablePath(FileFactory.getUpdatedFilePath(table.location.toString))
      relationIdentifier
    }
    dataMapSchema.getRelationIdentifier.setTablePath(tablePath)
    dataMapSchema.setParentTables(new util.ArrayList[RelationIdentifier](parentIdents.asJava))
    dataMapSchema.getProperties.put("full_refresh", fullRebuild.toString)
    try {
      DataMapStoreManager.getInstance().saveDataMapSchema(dataMapSchema)
    } catch {
      case ex: Exception =>
        val dropTableCommand = CarbonDropTableCommand(true,
          new Some[String](dataMapSchema.getRelationIdentifier.getDatabaseName),
          dataMapSchema.getRelationIdentifier.getTableName,
          true)
        dropTableCommand.run(sparkSession)
        throw ex
    }
  }

  private def validateMVQuery(sparkSession: SparkSession,
      logicalPlan: LogicalPlan): ModularPlan = {
    val dataMapProvider = DataMapManager.get().getDataMapProvider(null,
      new DataMapSchema("", DataMapClassProvider.MV.getShortName), sparkSession)
    var catalog = DataMapStoreManager.getInstance().getDataMapCatalog(dataMapProvider,
      DataMapClassProvider.MV.getShortName).asInstanceOf[SummaryDatasetCatalog]
    if (catalog == null) {
      catalog = new SummaryDatasetCatalog(sparkSession)
    }
    val modularPlan =
      catalog.mvSession.sessionState.modularizer.modularize(
        catalog.mvSession.sessionState.optimizer.execute(logicalPlan)).next().semiHarmonized

    // Only queries which can be select , predicate , join, group by and having queries.
    if (!modularPlan.isSPJGH)  {
      throw new UnsupportedOperationException("MV is not supported for this query")
    }
    val isValid = modularPlan match {
      case g: GroupBy =>
        // Make sure all predicates are present in projections.
        g.predicateList.forall{p =>
          g.outputList.exists{
            case a: Alias =>
              a.semanticEquals(p) || a.child.semanticEquals(p)
            case other => other.semanticEquals(p)
          }
        }
      case _ => true
    }
    if (!isValid) {
      throw new UnsupportedOperationException(
        "Group by columns must be present in project columns")
    }
    if (catalog.isMVWithSameQueryPresent(logicalPlan)) {
      throw new UnsupportedOperationException("MV with same query present")
    }

    var expressionValid = true
    modularPlan.transformExpressions {
      case coal@Coalesce(_) if coal.children.exists(
        exp => exp.isInstanceOf[AggregateExpression]) =>
        expressionValid = false
        coal
    }

    if (!expressionValid) {
      throw new UnsupportedOperationException("MV doesn't support Coalesce")
    }
    modularPlan
  }

  def getUpdatedName(name: String, counter: Int): String = {
    var updatedName = name.replace("(", "_")
      .replace(")", "")
      .replace(" ", "_")
      .replace("=", "")
      .replace(",", "")
      .replace(".", "_")
      .replace("`", "")
    if (updatedName.length >= CarbonCommonConstants.MAXIMUM_CHAR_LENGTH) {
      updatedName = updatedName.substring(0, 110) + CarbonCommonConstants.UNDERSCORE + counter
    }
    updatedName
  }

  private def updateColumnName(attr: Attribute, counter: Int): String = {
    val name = getUpdatedName(attr.name, counter)
    val value = attr.qualifier.map(qualifier => qualifier + "_" + name)
    if (value.nonEmpty) value.head else name
  }

  private def getTables(logicalPlan: LogicalPlan): Seq[CatalogTable] = {
    logicalPlan.collect {
      case l: LogicalRelation => l.catalogTable.get
    }
  }

  private def getLogicalRelation(logicalPlan: LogicalPlan): Seq[LogicalRelation] = {
    logicalPlan.collect {
      case l: LogicalRelation => l
    }
  }

  /**
   * Check if we can do incremental load on the mv table. Some cases like aggregation functions
   * which are present inside other expressions like sum(a)+sum(b) cannot be incremental loaded.
   */
  private def isFullReload(logicalPlan: LogicalPlan): Boolean = {
    var isFullReload = false
    logicalPlan.transformAllExpressions {
      case a: Alias => a
      case agg: AggregateExpression =>
        // If average function present then go for full refresh
        var reload = agg.aggregateFunction match {
          case avg: Average => true
          case _ => false
        }
        isFullReload = reload || isFullReload
        agg
      case c: Cast =>
        isFullReload = c.child.find {
          case agg: AggregateExpression => false
          case _ => false
        }.isDefined || isFullReload
        c
      case exp: Expression =>
        // Check any aggregation function present inside other expression.
        isFullReload = exp.find {
          case agg: AggregateExpression => true
          case _ => false
        }.isDefined || isFullReload
        exp
    }
    // TODO:- Remove this case when incremental datalaoding is supported for multiple tables
    logicalPlan.transformDown {
      case join@Join(l1, l2, jointype, condition) =>
        isFullReload = true
        join
    }
    isFullReload
  }

  /**
   * Validate mv timeseries query for timeseries column and granularity.
   * TimeSeries udf function will contain data type as TimeStamp/cast as TimeStamp
   *
   * @param logicalPlan   to be validated
   * @param dataMapSchema to check if it is lazy/non-lazy datamap
   * @return
   */
  private def validateMVTimeSeriesQuery(logicalPlan: LogicalPlan,
      dataMapSchema: DataMapSchema): (String, String) = {
    var timeSeriesColumn: String = null
    var granularity: String = null
    logicalPlan.transformExpressions {
      case alias@Alias(udf: ScalaUDF, _) =>
        if (udf.function.isInstanceOf[TimeSeriesFunction]) {
          if (null == timeSeriesColumn && null == granularity) {
            udf.children.collect {
              case attr: AttributeReference =>
                timeSeriesColumn = attr.name
              case l: Literal =>
                granularity = l.value.toString
              case c: Cast =>
                c.child match {
                  case attribute: AttributeReference =>
                    if (attribute.dataType.isInstanceOf[DateType]) {
                      timeSeriesColumn = attribute.name
                    }
                  case _ =>
                }
            }
          } else {
            udf.children.collect {
              case attr: AttributeReference =>
                if (!attr.name.equalsIgnoreCase(timeSeriesColumn)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                    "different timestamp columns")
                }
              case l: Literal =>
                if (!granularity.equalsIgnoreCase(l.value.toString)) {
                  throw new MalformedCarbonCommandException(
                    "Multiple timeseries udf functions are defined in Select statement with " +
                    "different granularities")
                }
            }
          }
        }
        alias
    }
    // timeseries column and granularity is not null, then validate
    if (null != timeSeriesColumn && null != granularity) {
      if (dataMapSchema.isLazy) {
        throw new MalformedCarbonCommandException(
          "MV TimeSeries queries does not support Lazy Rebuild")
      }
      TimeSeriesUtil.validateTimeSeriesGranularity(granularity)
    } else if (null == timeSeriesColumn && null != granularity) {
      throw new MalformedCarbonCommandException(
        "MV TimeSeries is only supported on Timestamp/Date column")
    }
    (timeSeriesColumn, granularity)
  }
}
