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

package org.apache.spark.sql.optimizer

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Dataset, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.{CarbonHiveMetadataUtil, CarbonInternalHiveMetadataUtil,
CarbonRelation}
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.spark.util.CarbonCostBasedOptimizer

/**
 * Carbon Optimizer to add dictionary decoder.
 */
class CarbonSecondaryIndexOptimizer(sparkSession: SparkSession) {

  /**
   * This method will identify whether provided filter column have any index table
   * if exist, it will transform the filter with join plan as follows
   *
   * eg., select * from  a where dt='20261201' and age='10' limit 10
   *
   * Assume 2 index tables
   * 1)index1 indexed on column(dt)
   * 2)index2 indexed on column(age)
   *
   * Plan will be transformed as follows
   *
   * with i1 as (select positionReference from index1 where dt='20261201'
   * group by positionReference),
   * with i2 as (select positionReference from index2 where age='10'),
   * with indexJion as (select positionReference from i1 join i2 on
   * i1.positionReference = i2.positionReference limit 10),
   * with index as (select positionReference from indexJoin group by positionReference)
   * select * from a join index on a.positionId = index.positionReference limit 10
   *
   * @param filter
   * @param indexableRelation
   * @param dbName
   * @param cols
   * @param limitLiteral
   * @return transformed logical plan
   */
  private def rewritePlanForSecondaryIndex(filter: Filter,
      indexableRelation: CarbonDatasourceHadoopRelation, dbName: String,
      cols: Seq[NamedExpression] = null, limitLiteral: Literal = null): LogicalPlan = {
    var originalFilterAttributes: Set[String] = Set.empty
    var filterAttributes: Set[String] = Set.empty
    var indexJoinedFilterAttributes: Set[String] = Set.empty
    var matchingIndexTables: Seq[String] = Seq.empty
    // all filter attributes are retrived
    filter.condition collect {
      case attr: AttributeReference =>
        originalFilterAttributes = originalFilterAttributes. + (attr.name.toLowerCase)
    }
    // Removed is Not Null filter from all filters and other attributes are selected
    // isNotNull filter will return all the unique values except null from table,
    // For High Cardinality columns, this filter is of no use, hence skipping it.
    removeIsNotNullAttribute(filter.condition) collect {
      case attr: AttributeReference =>
        filterAttributes = filterAttributes. + (attr.name.toLowerCase)
    }

    matchingIndexTables = CarbonCostBasedOptimizer.identifyRequiredTables(
      filterAttributes.asJava,
      CarbonInternalScalaUtil.getIndexes(indexableRelation).mapValues(_.toList.asJava).asJava)
      .asScala.toSeq

    if (matchingIndexTables.size == 0) {
      filter
    } else {
      var isPositionIDPresent = false
      val project: Seq[NamedExpression] = if (cols != null) {
        cols.foreach { element =>
          element match {
            case a@Alias(s: ScalaUDF, name)
              if (name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)) =>
              isPositionIDPresent = true
            case _ =>
          }
        }
        cols
      } else {
        filter.output
      }
      var mainTableDf = createDF(sparkSession, Project(project, filter))
      if (!isPositionIDPresent) {
        mainTableDf = mainTableDf.selectExpr("getPositionId() as positionId", "*")
      }
      var indexTablesDF: DataFrame = null
      matchingIndexTables.foreach(matchedTable => {

        val indexTableQualifier = Seq(dbName, matchedTable)
        val copyFilter = filter.copy(filter.condition, filter.child)
        val indexTablelogicalplan = sparkSession.sessionState.catalog
          .lookupRelation(TableIdentifier(matchedTable, Some(dbName))) match {
          case SubqueryAlias(_, l: LogicalRelation, _) => l
          case others => others
        }
        var indexTableAttributeMap: Map[String, AttributeReference] = null

        // collect index table columns
        indexTablelogicalplan collect {
          case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
            indexTableAttributeMap = l.output.map { attr => (attr.name.toLowerCase -> attr) }.toMap
        }
        val indexTableColumns = CarbonInternalScalaUtil.getIndexes(indexableRelation)
          .getOrElse(matchedTable, Array())
        // change filter condition to apply on index table

        var isPartialStringEnabled = CarbonProperties.getInstance
          .getProperty(CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING, "true")
          .equalsIgnoreCase("true")

        // When carbon.lookup.partialstring set to FALSE, if filter has startsWith then SI is
        // used eventhough combination of other filters like endsWith or Contains
        if (!isPartialStringEnabled) {
          isPartialStringEnabled = conditionsHasStartWith(copyFilter.condition,
            indexTableColumns.toSet);
        }

        val (tempIndexTableFilter, isIndexColumnPresent) = createIndexTableFilterCondition(
          copyFilter.condition, indexTableColumns.toSet, isPartialStringEnabled)

        if (isIndexColumnPresent) {
          val indexTableFilter = tempIndexTableFilter transformDown {
            case attr: AttributeReference =>
              val attrNew = indexTableAttributeMap.get(attr.name.toLowerCase()).get
              attrNew
          }
          indexJoinedFilterAttributes = indexJoinedFilterAttributes ++ indexTableColumns
          val positionReference =
            Seq(indexTableAttributeMap
              .get(CarbonInternalCommonConstants.POSITION_REFERENCE.toLowerCase())
              .get)
          // Add Filter on logicalRelation
          var planTransform: LogicalPlan = Filter(indexTableFilter, indexTablelogicalplan)
          // Add PositionReference Projection on Filter
          planTransform = Project(positionReference, planTransform)
          val indexTableDf = createDF(sparkSession, planTransform)
          indexTablesDF = if (indexTablesDF == null) {
            // Single index table selected case
            indexTableDf
          } else {
            // For multiple index table selection,
            // all index tables are joined before joining with main table
            indexTablesDF.join(indexTableDf,
              indexTablesDF(CarbonInternalCommonConstants.POSITION_REFERENCE) ===
              indexTableDf(CarbonInternalCommonConstants.POSITION_REFERENCE))
          }
          // When all the filter columns are joined from index table,
          // limit can be pushed down before grouping last index table as the
          // number of records selected will definitely return atleast 1 record
          val indexLogicalPlan = if (limitLiteral != null &&
                                     indexJoinedFilterAttributes.intersect(originalFilterAttributes)
                                       .size ==
                                     originalFilterAttributes.size) {
            Limit(limitLiteral, indexTablesDF.logicalPlan)
          } else {
            indexTablesDF.logicalPlan
          }
          // Add Group By on PositionReference after join
          indexTablesDF = createDF(sparkSession,
            Aggregate(positionReference, positionReference, indexLogicalPlan))
        }
      })
      // Incase column1 > column2, index table have only 1 column1,
      // then index join should not happen if only 1 index table is selected.
      if (indexTablesDF != null) {
        mainTableDf = mainTableDf.join(indexTablesDF,
          mainTableDf(CarbonInternalCommonConstants.POSITION_ID) ===
          indexTablesDF(CarbonInternalCommonConstants.POSITION_REFERENCE))
        mainTableDf.queryExecution.analyzed
      } else {
        filter
      }
    }
  }

  private def createDF(sparkSession: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    new Dataset[Row](sparkSession, logicalPlan, RowEncoder(logicalPlan.schema))
  }

  private def removeIsNotNullAttribute(condition: Expression): Expression = {
    val isPartialStringEnabled = CarbonProperties.getInstance
      .getProperty(CarbonInternalCommonConstants.ENABLE_SI_LOOKUP_PARTIALSTRING, "true")
      .equalsIgnoreCase("true")
    condition transform {
      case IsNotNull(child: AttributeReference) => Literal(true)
      // Like is possible only if user provides _ in between the string
      // _ in like means any single character wild card check.
      case plan if (CarbonInternalHiveMetadataUtil.checkNIUDF(plan)) => Literal(true)
      case Like(left: AttributeReference, right: Literal) if (!isPartialStringEnabled) => Literal(
        true)
      case EndsWith(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
      case Contains(left: AttributeReference,
      right: Literal) if (!isPartialStringEnabled) => Literal(true)
    }
  }

  private def conditionsHasStartWith(condition: Expression,
      indexTableColumns: Set[String]): Boolean = {
    condition match {
      case or@Or(left, right) =>
        val isIndexColumnUsedInLeft = conditionsHasStartWith(left, indexTableColumns)
        val isIndexColumnUsedInRight = conditionsHasStartWith(right, indexTableColumns)

        isIndexColumnUsedInLeft || isIndexColumnUsedInRight

      case and@And(left, right) =>
        val isIndexColumnUsedInLeft = conditionsHasStartWith(left, indexTableColumns)
        val isIndexColumnUsedInRight = conditionsHasStartWith(right, indexTableColumns)

        isIndexColumnUsedInLeft || isIndexColumnUsedInRight

      case _ => hasStartsWith(condition, indexTableColumns)
    }
  }

  private def hasStartsWith(condition: Expression, indexTableColumns: Set[String]): Boolean = {
    condition match {
      case Like(left: AttributeReference, right: Literal) => false
      case EndsWith(left: AttributeReference, right: Literal) => false
      case Contains(left: AttributeReference, right: Literal) => false
      case _ => true
    }
  }

  private def isConditionColumnInIndexTable(condition: Expression,
      indexTableColumns: Set[String], pushDownRequired: Boolean): Boolean = {
    // In case of Like Filter in OR, both the conditions should not be transformed
    // Incase of like filter in And, only like filter should be removed and
    // other filter should be transformed with index table

    // Incase NI condition with and, eg., NI(col1 = 'a') && col1 = 'b',
    // only col1 = 'b' should be pushed to index table.
    // Incase NI condition with or, eg., NI(col1 = 'a') || col1 = 'b',
    // both the condition should not be pushed to index table.

    val donotPushToSI = condition match {
      case Like(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case EndsWith(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case Contains(left: AttributeReference, right: Literal) if (!pushDownRequired) => true
      case plan if (CarbonInternalHiveMetadataUtil.checkNIUDF(plan)) => true
      case _ => false
    }
    if (donotPushToSI) {
      false
    } else {
      val attributes = condition collect {
        case attributeRef: AttributeReference => attributeRef
      }
      attributes
        .forall { attributeRef => indexTableColumns.contains(attributeRef.name.toLowerCase) }
    }
  }

  /**
   * This method will evaluate the condition and return new condition and flag
   * 1) Incase of or condition, all the columns in left & right are existing in index table,
   * then the condition will be pushed to index table and joined with main table
   * 2) Incase of and condition, if any of the left or right condition column matches with
   * index table column, then that particular condition will be pushed to index table.
   *
   * @param condition
   * @param indexTableColumns
   * @return newCondition can be pushed to index table & boolean to join with index table
   */
  private def createIndexTableFilterCondition(condition: Expression,
      indexTableColumns: Set[String], isPartialStringEnabled: Boolean): (Expression, Boolean) = {

    condition match {
      case or@Or(left, right) =>
        val (newLeft, isIndexColumnUsedInLeft) = createIndexTableFilterCondition(left,
          indexTableColumns, isPartialStringEnabled)
        val (newRight, isIndexColumnUsedInRight) = createIndexTableFilterCondition(right,
          indexTableColumns, isPartialStringEnabled)

        val newCondition = (isIndexColumnUsedInLeft, isIndexColumnUsedInRight) match {
          case (true, true) => or.copy(newLeft, newRight)
          case _ => condition
        }
        // Incase of or condition, if both left & right condition column belongs to same
        // index table, then push the condition, else or condition should not be pushed
        // even if index table exist for each column independently.
        (newCondition, isIndexColumnUsedInLeft && isIndexColumnUsedInRight)

      case and@And(left, right) =>
        val (newLeft, isIndexColumnUsedInLeft) = createIndexTableFilterCondition(left,
          indexTableColumns, isPartialStringEnabled)
        val (newRight, isIndexColumnUsedInRight) = createIndexTableFilterCondition(right,
          indexTableColumns, isPartialStringEnabled)

        val newCondition = (isIndexColumnUsedInLeft, isIndexColumnUsedInRight) match {
          case (true, true) => and.copy(newLeft, newRight)
          case (true, false) => newLeft
          case (false, true) => newRight
          case _ => condition
        }
        // Incase of and condition, if one of the left or right condition column have index table,
        // then that condition alone should be split and pushed to index table.
        // Incase all the columns exist in one index table, then complete condition should be
        // pushed to index table as join
        (newCondition, isIndexColumnUsedInLeft || isIndexColumnUsedInRight)

      case _ => (condition, isConditionColumnInIndexTable(condition,
        indexTableColumns,
        isPartialStringEnabled))
    }
  }

  /**
   * This method is used to determn whether limit has to be pushed down to secondary index or not.
   *
   * @param relation
   * @return false if carbon table is not an index table and update status file exists because
   *         we know delete has happened on table and there is no need to push down the filter.
   *         Otherwise true
   */
  private def isLimitPushDownRequired(relation: CarbonRelation): Boolean = {
    val carbonTable = relation.carbonTable
    lazy val updateStatusFileExists = FileFactory.getCarbonFile(carbonTable.getMetaDataFilepath,
      FileFactory.getFileType(carbonTable.getMetaDataFilepath)).listFiles()
      .exists(file => file.getName.startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME))
    (!CarbonInternalScalaUtil.isIndexTable(carbonTable) && updateStatusFileExists)
  }

  def transformFilterToJoin(plan: LogicalPlan): LogicalPlan = {
    var resolvedRelations: Set[LogicalPlan] = Set.empty
    val isRowDeletedInTableMap = scala.collection.mutable.Map.empty[String, Boolean]
    val transformedPlan = plan transform {
      case filter@Filter(condition, logicalRelation@MatchIndexableRelation(indexableRelation))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty &&
           !resolvedRelations.contains(logicalRelation) =>
        val reWrittenPlan = rewritePlanForSecondaryIndex(filter, indexableRelation,
          filter.child.asInstanceOf[LogicalRelation].relation
            .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.databaseName)
        if (reWrittenPlan.isInstanceOf[Join]) {
          resolvedRelations = resolvedRelations. + (logicalRelation)
          addProjectForStarQuery(filter.output, reWrittenPlan)
        } else {
          filter
        }
      case projection@Project(cols, filter@Filter(condition,
      logicalRelation@MatchIndexableRelation(indexableRelation)))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty &&
           !resolvedRelations.contains(logicalRelation) =>
        val reWrittenPlan = rewritePlanForSecondaryIndex(filter, indexableRelation,
          filter.child.asInstanceOf[LogicalRelation].relation
            .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.databaseName, cols)
        // If Index table is matched, join plan will be returned.
        // Adding projection over join to return only selected columns from query.
        // Else all columns from left & right table will be returned in output columns
        if (reWrittenPlan.isInstanceOf[Join]) {
          resolvedRelations = resolvedRelations. + (logicalRelation)
          Project(projection.output, reWrittenPlan)
        } else {
          projection
        }
      // When limit is provided in query, this limit literal can be pushed down to index table
      // if all the filter columns have index table, then limit can be pushed down before grouping
      // last index table, as number of records returned after join where unique and it will
      // definitely return atleast 1 record.
      case limit@Limit(literal: Literal,
      filter@Filter(condition, logicalRelation@MatchIndexableRelation(indexableRelation)))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty &&
           !resolvedRelations.contains(logicalRelation) =>
        val carbonRelation = filter.child.asInstanceOf[LogicalRelation].relation
          .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        val uniqueTableName = s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }"
        if (!isRowDeletedInTableMap
          .contains(s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }")) {
          isRowDeletedInTableMap.put(uniqueTableName, isLimitPushDownRequired(carbonRelation))
        }
        val reWrittenPlan = if (isRowDeletedInTableMap(uniqueTableName)) {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, limitLiteral = literal)
        } else {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName)
        }
        if (reWrittenPlan.isInstanceOf[Join]) {
          resolvedRelations = resolvedRelations. + (logicalRelation)
          Limit(literal, addProjectForStarQuery(limit.output, reWrittenPlan))
        } else {
          limit
        }
      case limit@Limit(literal: Literal, projection@Project(cols, filter@Filter(condition,
      logicalRelation@MatchIndexableRelation(indexableRelation))))
        if !condition.isInstanceOf[IsNotNull] &&
           CarbonInternalScalaUtil.getIndexes(indexableRelation).nonEmpty &&
           !resolvedRelations.contains(logicalRelation) =>
        val carbonRelation = filter.child.asInstanceOf[LogicalRelation].relation
          .asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation
        val uniqueTableName = s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }"
        if (!isRowDeletedInTableMap
          .contains(s"${ carbonRelation.databaseName }.${ carbonRelation.tableName }")) {
          isRowDeletedInTableMap.put(uniqueTableName, isLimitPushDownRequired(carbonRelation))
        }
        val reWrittenPlan = if (isRowDeletedInTableMap(uniqueTableName)) {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, cols, limitLiteral = literal)
        } else {
          rewritePlanForSecondaryIndex(filter, indexableRelation,
            carbonRelation.databaseName, cols)
        }
        if (reWrittenPlan.isInstanceOf[Join]) {
          resolvedRelations = resolvedRelations. + (logicalRelation)
          Limit(literal, Project(projection.output, reWrittenPlan))
        } else {
          limit
        }
    }
    transformedPlan.transform {
      case filter: Filter =>
        Filter(CarbonInternalHiveMetadataUtil.transformToRemoveNI(filter.condition), filter.child)
    }
  }

  private def addProjectForStarQuery(output: Seq[Attribute],
      reWrittenPlan: LogicalPlan): LogicalPlan = {
    Project(output, reWrittenPlan)
  }
}

object MatchIndexableRelation {

  type ReturnType = (CarbonDatasourceHadoopRelation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      case l: LogicalRelation if (l.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
        Some(l.relation.asInstanceOf[CarbonDatasourceHadoopRelation])
      case _ => None
    }
  }
}
