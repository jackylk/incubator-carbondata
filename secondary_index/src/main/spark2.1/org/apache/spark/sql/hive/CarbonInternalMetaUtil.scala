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
package org.apache.spark.sql.hive

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, LogicalPlan}
import org.apache.spark.util.CarbonInternalScalaUtil
import org.apache.spark.sql.CarbonExpressions.{CarbonSubqueryAlias => SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.acl.ACLInterface

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 *
 */
object CarbonInternalMetaUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   *
   * @param otherRDDPlan
   * @param session
   * @return
   */
  def canPushDown(otherRDDPlan: LogicalPlan,
    session: SparkSession): Boolean = {
    otherRDDPlan match {
      case BroadcastHint(p) => true
      case p if session.sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
        p.stats(session.sqlContext.conf).sizeInBytes <=
          session.sqlContext.conf.autoBroadcastJoinThreshold =>
        LOGGER.info("canPushDownJoin statistics:" + p.stats(session.sqlContext.conf).sizeInBytes)
        true
      case plan if (CarbonInternalScalaUtil.checkIsIndexTable(plan)) => true
      case _ => false
    }
  }

  def retrieveRelation(plan: LogicalPlan)(sparkSession: SparkSession):
  CarbonDatasourceHadoopRelation = {
    plan match {
      case SubqueryAlias(alias, l: LogicalRelation) if (l.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case l: LogicalRelation if (l.relation
        .isInstanceOf[CarbonDatasourceHadoopRelation]) => l.relation
        .asInstanceOf[CarbonDatasourceHadoopRelation]
      case _ => null
    }
  }

  def retrievePlan(plan: LogicalPlan)(sparkSession: SparkSession):
  LogicalRelation = {
    plan match {
      case SubqueryAlias(alias, l: LogicalRelation)
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] => l
      case _ => null
    }
  }
  /**
   * return's aclInterface from CarbonACLSessionCatalog
   *
   * @param sparkSession
   * @return
   */
  def getACLInterface(sparkSession: SparkSession) : ACLInterface = {
    sparkSession.sessionState.asInstanceOf[CarbonSessionState].aclInterface
  }

  /**
    *
    * @param sparkSession
    * @return
    */
  def getClientUser(sparkSession: SparkSession): String = {
    sparkSession.sessionState.catalog.getClientUser
  }
}