/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.spark.sql.execution.strategy

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.security.AccessControlException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, IntegerLiteral}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, _}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.{BroadCastFilterPushJoin, BroadCastSIFilterPushJoin, BuildLeft, BuildRight}
import org.apache.spark.sql.hive.{CarbonInternalMetaUtil, MatchLogicalRelation}
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants
import org.apache.carbondata.spark.rdd.CarbonScanRDD

/**
 * Carbon specific optimization for late decode (like broadcast join)
 */
private[sql] class CarbonInternalLateDecodeStrategy extends CarbonLateDecodeStrategy {

  val LOGGER = LogServiceFactory.getLogService("CarbonLateDecodeStrategy")

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {

    try {
      plan match {
        case GlobalLimit(IntegerLiteral(limit), LocalLimit(IntegerLiteral(limitValue),
        p@PhysicalOperation(projects, filters, l: LogicalRelation)))
          if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
          val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
          GlobalLimitExec(limit, LocalLimitExec(limitValue,
            pruneFilterProject(
              l,
              projects,
              filters,
              (a, f, needDecoder, p) => toCatalystRDD(l, a, relation.buildScan(
                a.map(_.name).toArray, filters, projects, f, p), needDecoder)))) :: Nil
        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition,
        left, right)
          if (isCarbonPlan(left) &&
              canPushDownJoin(right, condition)) =>
          LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
          val carbon = apply(left).head
          val pushedDownJoin = if (CarbonInternalScalaUtil.checkIsIndexTable(right)) {
            // in case of SI Filter push join remove projection list from the physical plan
            // no need to have the project list in the main table physical plan execution
            // only join uses the projection list
            var carbonChild = carbon match {
              case projectExec: ProjectExec =>
                projectExec.child
              case _ =>
                carbon
            }
            // check if the outer and the inner project are matching, only then remove project
            if (left.isInstanceOf[Project]) {
              val leftOutput = left.output
                .filterNot(attr => attr.name
                  .equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_ID))
                .map(c => (c.name.toLowerCase, c.dataType))
              val childOutput = carbonChild.output
                .filterNot(attr => attr.name
                  .equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_ID))
                .map(c => (c.name.toLowerCase, c.dataType))
              if (!leftOutput.equals(childOutput)) {
                // if the projection list and the scan list are different(in case of alias)
                // we should not skip the project, so we are taking the original plan with project
                carbonChild = carbon
              }
            }
            BroadCastSIFilterPushJoin(
              leftKeys: Seq[Expression],
              rightKeys: Seq[Expression],
              Inner,
              BuildRight,
              carbonChild,
              planLater(right),
              condition)
          } else {
            BroadCastFilterPushJoin(
              leftKeys: Seq[Expression],
              rightKeys: Seq[Expression],
              Inner,
              BuildRight,
              carbon,
              planLater(right),
              condition)
          }
          condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left,
        right)
          if (isCarbonPlan(right) &&
              canPushDownJoin(left, condition)) =>
          LOGGER.info(s"pushing down for ExtractEquiJoinKeys:left")
          val carbon = planLater(right)

          val pushedDownJoin = if (CarbonInternalScalaUtil.checkIsIndexTable(left)) {
            BroadCastSIFilterPushJoin(
              leftKeys: Seq[Expression],
              rightKeys: Seq[Expression],
              Inner,
              BuildLeft,
              planLater(left),
              carbon,
              condition)
          } else {
            BroadCastFilterPushJoin(
              leftKeys: Seq[Expression],
              rightKeys: Seq[Expression],
              Inner,
              BuildLeft,
              planLater(left),
              carbon,
              condition)
          }
          condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
        // query with in filter as subquery will be optimized and pushed as leftSemiExist join,
        // In this case, right table output values will be pushed as IN filter to left table
        // eg., select * from a where a.id in (select id from b)
        case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition,
        left, right)
          if (isLeftSemiExistPushDownEnabled &&
              isAllCarbonPlan(left) && isAllCarbonPlan(right)) =>
          LOGGER.info(s"pushing down for ExtractEquiJoinKeysLeftSemiExist:right")
          val carbon = planLater(left)
          val pushedDownJoin = BroadCastSIFilterPushJoin(
            leftKeys: Seq[Expression],
            rightKeys: Seq[Expression],
            LeftSemi,
            BuildRight,
            carbon,
            planLater(right),
            condition)
          condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
        case _ => super.apply(plan)
      }
    } catch {
      case e: AccessControlException =>
        LOGGER.error("Missing Privileges:" + e.getMessage)
        throw new AnalysisException("Missing Privileges")
      case others => throw others
    }
  }

  private[this] def toCatalystRDD(
      relation: LogicalRelation,
      output: Seq[Attribute],
      rdd: RDD[InternalRow],
      needDecode: ArrayBuffer[AttributeReference]):
  RDD[InternalRow] = {
    if (needDecode.nonEmpty) {
      rdd.asInstanceOf[CarbonScanRDD[InternalRow]].setVectorReaderSupport(false)
      getDecoderRDD(relation, needDecode, rdd, output)
    } else {
      rdd.asInstanceOf[CarbonScanRDD[InternalRow]]
        .setVectorReaderSupport(supportBatchedDataSource(relation.relation.sqlContext, output))
      rdd
    }
  }

  private def isCarbonPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
        true
      case PhysicalOperation(_, _,
      MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case LogicalFilter(_, MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case _ => false
    }
  }

  private def canPushDownJoin(otherRDDPlan: LogicalPlan,
      joinCondition: Option[Expression]): Boolean = {
    val session = SparkSession.getActiveSession.get
    val pushDowmJoinEnabled = session.sparkContext.getConf
      .getBoolean("spark.carbon.pushdown.join.as.filter", defaultValue = true)
    if (!pushDowmJoinEnabled) {
      return false
    }
    CarbonInternalMetaUtil.canPushDown(otherRDDPlan, session)
  }

  private def isLeftSemiExistPushDownEnabled: Boolean = {
    CarbonProperties.getInstance.getProperty(
      CarbonInternalCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER,
      CarbonInternalCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT)
      .equalsIgnoreCase("true")
  }

  private def isAllCarbonPlan(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    !allRelations.exists(x => !x.relation.isInstanceOf[CarbonDatasourceHadoopRelation])
  }

  override def getRequestedColumns(relation: LogicalRelation,
    projectsAttr: Seq[Attribute],
    filterSet: AttributeSet,
    handledSet: AttributeSet,
    newProjectList: Seq[Attribute],
    updatedProjects: Seq[Expression]): (Seq[Attribute], Seq[Expression]) = {
    val sparkSession = SparkSession.getActiveSession.get
    val pushDownJoinEnabled = sparkSession.sparkContext.getConf
      .getBoolean("spark.carbon.pushdown.join.as.filter", defaultValue = true)

    // positionId column can be added in two cases
    // case 1: SI pushdown case, SI rewritten plan adds positionId column
    // case 2: if the user requested positionId column thru getPositionId() UDF
    // positionId column should be removed only in case 1, as it is manually added
    // Below code is added to handle case 2. But getPositionId() UDF is almost used only for testing
    val isPositionIDRequested = relation.catalogTable match {
      case Some(table) =>
        val tblProperties = CarbonEnv.getCarbonTable(table.identifier)(sparkSession).getTableInfo
          .getFactTable
          .getTableProperties
        val isPosIDRequested = if (tblProperties.containsKey("isPositionIDRequested")) {
          val flag = java.lang.Boolean.parseBoolean(tblProperties.get("isPositionIDRequested"))
          tblProperties.remove("isPositionIDRequested")
          flag
        } else {
          false
        }
        isPosIDRequested
      case _ => false
    }
    // remove positionId col only if pushdown is enabled and
    // positionId col is not requested in the query
    if (pushDownJoinEnabled && !isPositionIDRequested) {
      ((projectsAttr.to[scala.collection.mutable.LinkedHashSet] ++ filterSet -- handledSet)
         .map(relation.attributeMap).toSeq ++ newProjectList
         .filterNot(attr => attr.name
           .equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_ID)), updatedProjects
        .filterNot(attr => attr.isInstanceOf[AttributeReference] &&
                           attr.asInstanceOf[AttributeReference].name
                             .equalsIgnoreCase(CarbonInternalCommonConstants.POSITION_ID)))
    } else {
      ((projectsAttr.to[scala.collection.mutable.LinkedHashSet] ++ filterSet -- handledSet)
         .map(relation.attributeMap).toSeq ++ newProjectList, updatedProjects)
    }
  }
}
