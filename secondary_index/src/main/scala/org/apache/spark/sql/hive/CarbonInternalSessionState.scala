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

import org.apache.spark.sql._
import org.apache.spark.sql.acl._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.InternalDDLStrategy
import org.apache.spark.sql.events._
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlan}
import org.apache.spark.sql.execution.strategy.CarbonInternalLateDecodeStrategy
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonSITransformationRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonInternalSparkSqlParser

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.util.CarbonPluginProperties
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.{LoadTableAbortExecutionEvent, LoadTablePostExecutionEvent, LoadTablePreExecutionEvent, LoadTablePreStatusUpdateEvent}
import org.apache.carbondata.spark.acl.ACLFileFactory

/**
 *
 */
class CarbonInternalSessionState(sparkSession: SparkSession)
  extends CarbonSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonInternalSparkSqlParser(conf,
    sparkSession)

  override def extraStrategies: Seq[Strategy] = {
    super.extraStrategies ++
    Seq(new CarbonInternalLateDecodeStrategy, new InternalDDLStrategy(sparkSession))
  }

  override def extraOptimizations: Seq[Rule[LogicalPlan]] = {
    Seq()
  }

  override lazy val otherPrepareRules: Seq[Rule[SparkPlan]] = {
    Seq(CarbonPrivCheck(sparkSession, catalog, aclInterface))
  }

  override def customPreOptimizationRules: Seq[(String, Int, Seq[Rule[LogicalPlan]])] = {
    super.customPreOptimizationRules :+
    ("Carbon PreOptimizer Rule", 1, Seq(new CarbonPreOptimizerRule))
  }

  override def customPostHocOptimizationRules: Seq[(String, Int, Seq[Rule[LogicalPlan]])] = {
    super.customPostHocOptimizationRules :+
    ("Carbon PostOptimizer Rule", conf.optimizerMaxIterations,
      Seq(new CarbonIUDRule,
        new CarbonUDFTransformRule,
        new CarbonSITransformationRule(sparkSession),
        new CarbonLateDecodeRule))
  }

  override lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods) {

    override def extendedAggOptimizationRules: Seq[Rule[LogicalPlan]] = {
      super.extendedAggOptimizationRules ++ customAggOptimizationRules
    }

    override def preOptimizationBatches: Seq[Batch] = {
      super.preOptimizationBatches ++
      customPreOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }

    // TODO shouldnot be there
    override def tailOptimizationBatches: Seq[Batch] = {
      super.tailOptimizationBatches ++
      customTailOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }

    // TODO shouldnot be there
    override def postHocOptimizationBatched: Seq[Batch] = {
      super.postHocOptimizationBatched ++
      customPostHocOptimizationRules.map { case (desc, iterTimes, rules) =>
        Batch(desc, FixedPoint(iterTimes), rules: _*)
      }
    }
  }

  // initialize all listeners
  CarbonInternalSessionState.init

  class CarbonPreOptimizerRule extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    }
  }

  override def extendedAnalyzerRules: Seq[Rule[LogicalPlan]] = {
    CarbonAccessControlRules(sparkSession, catalog, aclInterface) :: Nil
  }
}

// Register all the required listeners using the singleton instance as the listeners
// need to be registered only once
object CarbonInternalSessionState {
  var initialized = false

  def init: Unit = {
    if (!initialized) {
      // register internal carbon property to propertySet
      CarbonPluginProperties.validateAndLoadDefaultInternalProperties()

      val operationListenerBus = OperationListenerBus.getInstance()

      // Listeners added for blocking features(insert overwrite, bucketing, partition, complex type)
      operationListenerBus
        .addListener(classOf[CreateTablePreExecutionEvent],
          new BlockEventListener)
      operationListenerBus
        .addListener(classOf[LoadTablePreExecutionEvent],
          new BlockEventListener)

      // ACL Listeners
      FileFactory.setFileTypeInerface(new ACLFileFactory())
      operationListenerBus
        .addListener(classOf[LoadTablePreExecutionEvent],
          new ACLLoadEventListener.ACLPreLoadEventListener)
      operationListenerBus
        .addListener(classOf[LoadTablePostExecutionEvent],
          new ACLLoadEventListener.ACLPostLoadEventListener)
      operationListenerBus
        .addListener(classOf[LoadTableAbortExecutionEvent],
          new ACLLoadEventListener.ACLAbortLoadEventListener)

      operationListenerBus
        .addListener(classOf[CreateTablePreExecutionEvent],
          new ACLCreateTableEventListener.ACLPreCreateTableEventListener)
      operationListenerBus
        .addListener(classOf[CreateTablePostExecutionEvent],
          new ACLCreateTableEventListener.ACLPostCreateTableEventListener)
      operationListenerBus
        .addListener(classOf[CreateTableAbortExecutionEvent],
          new ACLCreateTableEventListener.ACLAbortCreateTableEventListener)

      operationListenerBus
        .addListener(classOf[AlterTableAddColumnPreEvent],
          new ACLAlterTableAddColumnEventListener.ACLPreAlterTableAddColumnEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableAddColumnPostEvent],
          new ACLAlterTableAddColumnEventListener.ACLPostAlterTableAddColumnEventListener)

      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPreEvent],
          new ACLAlterTableDropColumnEventListener.ACLPreAlterTableDropColumnEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPostEvent],
          new ACLAlterTableDropColumnEventListener.ACLPostAlterTableDropColumnEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnAbortEvent],
          new ACLAlterTableDropColumnEventListener.ACLAbortAlterTableDropColumnEventListener)

      operationListenerBus
        .addListener(classOf[AlterTableDataTypeChangePreEvent],
          new ACLAlterTableDataTypeChangeEventListener
          .ACLPreAlterTableDataTypeChangeEventListener)

      operationListenerBus
        .addListener(classOf[AlterTableDataTypeChangePostEvent],
          new ACLAlterTableDataTypeChangeEventListener
          .ACLPostAlterTableDataTypeChangeEventListener)

      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePreEvent],
          new ACLDeleteSegmentByDateEventListener.ACLPreDeleteSegmentByDateEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePostEvent],
          new ACLDeleteSegmentByDateEventListener.ACLPostDeleteSegmentByDateEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDateAbortEvent],
          new ACLDeleteSegmentByDateEventListener.ACLAbortDeleteSegmentByDateEventListener)

      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPreEvent],
          new ACLDeleteSegmentByIdEventListener.ACLPreDeleteSegmentByIdEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPostEvent],
          new ACLDeleteSegmentByIdEventListener.ACLPostDeleteSegmentByIdEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdAbortEvent],
          new ACLDeleteSegmentByIdEventListener.ACLAbortDeleteSegmentByIdEventListener)

      operationListenerBus
        .addListener(classOf[CleanFilesPreEvent],
          new ACLCleanFilesEventListener.ACLPreCleanFilesEventListener)
      operationListenerBus
        .addListener(classOf[CleanFilesPostEvent],
          new ACLCleanFilesEventListener.ACLPostCleanFilesEventListener)
      operationListenerBus
        .addListener(classOf[CleanFilesAbortEvent],
          new ACLCleanFilesEventListener.ACLAbortCleanFilesEventListener)

      operationListenerBus
        .addListener(classOf[AlterTableCompactionPreEvent],
          new ACLCompactionEventListener.ACLPreCompactionEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableCompactionPostEvent],
          new ACLCompactionEventListener.ACLPostCompactionEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableCompactionAbortEvent],
          new ACLCompactionEventListener.ACLAbortCompactionEventListener)

      operationListenerBus
        .addListener(classOf[CreateDatabasePreExecutionEvent],
          new ACLCreateDatabaseListener.ACLPreCreateDatabaseListener)
      operationListenerBus
        .addListener(classOf[CreateDatabasePostExecutionEvent],
          new ACLCreateDatabaseListener.ACLPostCreateDatabaseListener)
      operationListenerBus
        .addListener(classOf[CreateDatabaseAbortExecutionEvent],
          new ACLCreateDatabaseListener.ACLAbortCreateDatabaseListener)

      operationListenerBus
        .addListener(classOf[DeleteFromTablePreEvent],
          new ACLIUDDeleteEventListener.ACLPreIUDDeleteEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTablePostEvent],
          new ACLIUDDeleteEventListener.ACLPostIUDDeleteEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTableAbortEvent],
          new ACLIUDDeleteEventListener.ACLAbortIUDDeleteEventListener)

      operationListenerBus
        .addListener(classOf[UpdateTablePreEvent],
          new ACLIUDUpdateEventListener.ACLPreIUDUpdateEventListener)
      operationListenerBus
        .addListener(classOf[UpdateTablePostEvent],
          new ACLIUDUpdateEventListener.ACLPostIUDUpdateEventListener)
      operationListenerBus
        .addListener(classOf[UpdateTableAbortEvent],
          new ACLIUDUpdateEventListener.ACLAbortIUDUpdateEventListener)

      operationListenerBus
        .addListener(classOf[CarbonEnvInitPreEvent],
          new CarbonEnvInitPreEventListener)

      // SI Listeners
      operationListenerBus
        .addListener(classOf[LoadTablePreStatusUpdateEvent], new SILoadEventListener)
      operationListenerBus
        .addListener(classOf[LookupRelationPostEvent], new SIRefreshEventListener)
      // TODO: get create relation event
      operationListenerBus
        .addListener(classOf[CreateCarbonRelationPostEvent], new
        CreateCarbonRelationEventListener)
      operationListenerBus
        .addListener(classOf[DropTablePreEvent], new SIDropEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableDropColumnPreEvent], new AlterTableDropColumnEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableRenamePostEvent], new AlterTableRenameEventListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByIdPostEvent], new DeleteSegmentByIdListener)
      operationListenerBus
        .addListener(classOf[DeleteSegmentByDatePostEvent], new DeleteSegmentByDateListener)
      operationListenerBus
        .addListener(classOf[CleanFilesPostEvent], new CleanFilesPostEventListener)
      operationListenerBus
        .addListener(classOf[AlterTableCompactionPreStatusUpdateEvent],
          new AlterTableCompactionPostEventListener)
      operationListenerBus
        .addListener(classOf[UpdateTablePreEvent], new UpdateTablePreEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTablePostEvent], new DeleteFromTableEventListener)
      operationListenerBus
        .addListener(classOf[DeleteFromTablePreEvent], new DeleteFromTableEventListener)
      // refresh table listner
      operationListenerBus
        .addListener(classOf[RefreshTablePreExecutionEvent],
          new ACLRefreshTableEventListener.ACLPreRefreshTableEventListener)
      operationListenerBus
        .addListener(classOf[RefreshTablePostExecutionEvent],
          new ACLRefreshTableEventListener.ACLPostRefreshTableEventListener)
      initialized = true
    }
  }
}
