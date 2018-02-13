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

package org.apache.spark.sql.execution.strategy

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.security.AccessControlException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{AnalysisException, CarbonDatasourceHadoopRelation,
CarbonDictionaryCatalystDecoder, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression,
IntegerLiteral}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, BroadcastHint, Filter => LogicalFilter}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, LogicalPlan}
import org.apache.spark.sql.execution.{FilterExec, GlobalLimitExec, LocalLimitExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.joins.{BroadCastFilterPushJoin, BroadCastSIFilterPushJoin,
BuildLeft, BuildRight}
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
                a.map(_.name).toArray, f, p), needDecoder)))) :: Nil
        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition,
        left, right)
          if (isCarbonPlan(left) &&
              canPushDownJoin(right, condition)) =>
          LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
          val carbon = apply(left).head
          val pushedDownJoin = if (checkIsIndexTable(right)) {
            BroadCastSIFilterPushJoin(
              leftKeys: Seq[Expression],
              rightKeys: Seq[Expression],
              Inner,
              BuildRight,
              carbon,
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

          val pushedDownJoin = if (checkIsIndexTable(left)) {
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
      rdd.asInstanceOf[CarbonScanRDD].setVectorReaderSupport(false)
      getDecoderRDD(relation, needDecode, rdd, output)
    } else {
      rdd.asInstanceOf[CarbonScanRDD]
        .setVectorReaderSupport(supportBatchedDataSource(relation.relation.sqlContext, output))
      rdd
    }
  }

  private def isCarbonPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, _, child) =>
        true
      case PhysicalOperation(_, _,
      LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case LogicalFilter(_, LogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
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
    otherRDDPlan match {
      case BroadcastHint(p) => true
      case p if session.sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
                p.stats(session.sqlContext.conf).sizeInBytes <=
                session.sqlContext.conf.autoBroadcastJoinThreshold =>
        LOGGER.info("canPushDownJoin statistics:" + p.stats(session.sqlContext.conf).sizeInBytes)
        true
      case plan if (checkIsIndexTable(plan)) => true
      case _ => false
    }
  }

  private def checkIsIndexTable(plan: LogicalPlan): Boolean = {
    plan match {
      case Aggregate(_, _, plan) if (isIndexTablesJoin(plan)) => true
      case _ => false
    }
  }

  private def isIndexTablesJoin(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    !allRelations.exists(x =>
      !(x.relation.isInstanceOf[CarbonDatasourceHadoopRelation]
        && CarbonInternalScalaUtil
          .isIndexTable(x.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable)))
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
}