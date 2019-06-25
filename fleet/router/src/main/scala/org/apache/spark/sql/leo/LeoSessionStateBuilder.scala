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

package org.apache.spark.sql.leo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, FindDataSourceTable, HiveOnlyCheck, PreWriteCheck, PreprocessTableCreation, PreprocessTableInsertion, ResolveSQLOnFile}
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonAnalyzer, CarbonIUDAnalysisRule, CarbonPreInsertionCasts, CarbonSessionStateBuilder, DetermineTableStats, HiveAnalysis, RelationConversions, ResolveHiveSerdeTable}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.parser.CarbonSparkSqlParser

class LeoSessionStateBuilder(
    sparkSession: SparkSession,
    parentState: Option[SessionState] = None)
  extends CarbonSessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser = new LeoSqlParser(conf, sparkSession)

  experimentalMethods.extraStrategies =
    Seq(
      new LeoDDLStrategy(sparkSession),
      new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession)
    )

  override protected def analyzer: Analyzer = new CarbonAnalyzer(catalog, conf, sparkSession,
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new ResolveHiveSerdeTable(session) +:
        new FindDataSourceTable(session) +:
        new ResolveSQLOnFile(session) +:
        new CarbonIUDAnalysisRule(sparkSession) +:
        new CarbonPreInsertionCasts(sparkSession) +:
        new LeoTVFAnalyzerRule(sparkSession) +: customResolutionRules

      override val extendedCheckRules: Seq[LogicalPlan => Unit] =
        PreWriteCheck :: HiveOnlyCheck :: Nil

      override val postHocResolutionRules: Seq[Rule[LogicalPlan]] =
        new DetermineTableStats(session) +:
        RelationConversions(conf, catalog) +:
        PreprocessTableCreation(session) +:
        PreprocessTableInsertion(conf) +:
        DataSourceAnalysis(conf) +:
        HiveAnalysis +:
        customPostHocResolutionRules
    }
  )

  override protected def newBuilder = new LeoSessionStateBuilder(_, _)
}
