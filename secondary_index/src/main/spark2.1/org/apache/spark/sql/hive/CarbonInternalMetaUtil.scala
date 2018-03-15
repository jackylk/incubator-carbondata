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
}
