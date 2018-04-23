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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.acl._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{DropTablePreEvent => _, _}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.command.InternalDDLStrategy
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableDataTypeChangeCommand}
import org.apache.spark.sql.execution.command.table.CarbonShowTablesCommand
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsModel, AlterTableDataTypeChangeModel}
import org.apache.spark.sql.execution.datasources.{DataSourceAnalysis, PreprocessTableCreation, PreprocessTableInsertion, _}
import org.apache.spark.sql.execution.strategy.{CarbonInternalLateDecodeStrategy, CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.acl.ACLInterface
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonSITransformationRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.util.CarbonScalaUtil

/**
  * This class will have carbon catalog and refresh the relation from cache if the carbontable in
  * carbon catalog is not same as cached carbon relation's carbon table
  *
  * @param externalCatalog
  * @param globalTempViewManager
  * @param sparkSession
  * @param functionResourceLoader
  * @param functionRegistry
  * @param conf
  * @param hadoopConf
  */
class CarbonACLSessionCatalog(
  externalCatalog: HiveACLExternalCatalog,
  globalTempViewManager: GlobalTempViewManager,
  functionRegistry: FunctionRegistry,
  sparkSession: SparkSession,
  conf: SQLConf,
  hadoopConf: Configuration,
  parser: ParserInterface,
  functionResourceLoader: FunctionResourceLoader)
  extends HiveACLSessionCatalog(
    externalCatalog,
    globalTempViewManager,
    new HiveMetastoreCatalog(sparkSession),
    functionRegistry,
    conf,
    hadoopConf,
    parser,
    functionResourceLoader
  ) with CarbonSessionCatalog {
  lazy val carbonEnv = {
    val env = new CarbonEnv
    env.init(sparkSession)
    env
  }

  var hiveClient: HiveClient = null
  var aclInterface: ACLInterface = null

  def setHiveClient(client: HiveClient): Unit = {
    hiveClient = client
  }

  /**
   * to set acl interface
   * @param aCLInterface
   */
  def setACLInterface(aCLInterface: ACLInterface): Unit = {
    aclInterface = aCLInterface
  }
  /**
   * return's the corbonEnv instance
    *
    * @return
   */
  override def getCarbonEnv(): CarbonEnv = {
    carbonEnv
  }

  // Initialize all listeners to the Operation bus.
  CarbonEnv.init(sparkSession)

  override def lookupRelation(name: TableIdentifier): LogicalPlan = {
    val rtnRelation = super.lookupRelation(name)
    val isRelationRefreshed =
      CarbonSessionUtil.refreshRelation(rtnRelation, name)(sparkSession)
    if (isRelationRefreshed) {
      super.lookupRelation(name)
    } else {
      rtnRelation
    }
  }

  /**
   * returns hive client from HiveExternalCatalog
   *
   * @return
   */
  def getClient(): org.apache.spark.sql.hive.client.HiveClient = {
    hiveClient
  }

  /**
   * Return's the aclinterface
   * @return
   */
  def getACLInterface(): ACLInterface = {
    aclInterface
  }


  override def createPartitions(
    tableName: TableIdentifier,
    parts: Seq[CatalogTablePartition],
    ignoreIfExists: Boolean): Unit = {
    try {
      val table = CarbonEnv.getCarbonTable(tableName)(sparkSession)
      val updatedParts = CarbonScalaUtil.updatePartitions(parts, table)
      super.createPartitions(tableName, updatedParts, ignoreIfExists)
    } catch {
      case e: Exception =>
        super.createPartitions(tableName, parts, ignoreIfExists)
    }
  }

  /**
   * This is alternate way of getting partition information. It first fetches all partitions from
   * hive and then apply filter instead of querying hive along with filters.
   *
   * @param partitionFilters
   * @param sparkSession
   * @param identifier
   * @return
   */
  override def getPartitionsAlternate(partitionFilters: Seq[Expression], sparkSession: SparkSession,
    identifier: TableIdentifier): Seq[CatalogTablePartition] = {
    CarbonSessionUtil.prunePartitionsByFilter(partitionFilters, sparkSession, identifier)
  }
  /**
   * Update the storageformat with new location information
   */
  override def updateStorageLocation(
    path: Path,
    storage: CatalogStorageFormat): CatalogStorageFormat = {
    storage.copy(locationUri = Some(path.toUri))
  }
}


class CarbonACLInternalSessionStateBuilder(sparkSession: SparkSession,
  parentState: Option[SessionState] = None)
  extends HiveACLSessionStateBuilder(sparkSession, parentState) {

  override lazy val sqlParser: ParserInterface = new CarbonInternalSparkSqlParser(conf,
    sparkSession
  )

  experimentalMethods.extraStrategies =
    Seq(new StreamingTableStrategy(sparkSession),
      new CarbonLateDecodeStrategy,
      new DDLStrategy(sparkSession),
      new CarbonInternalLateDecodeStrategy,
      new InternalDDLStrategy(sparkSession)
    )

  experimentalMethods.extraOptimizations = Seq(new CarbonIUDRule,
    new CarbonUDFTransformRule,
    new CarbonSITransformationRule(sparkSession),
    new CarbonLateDecodeRule
  )

  var preExecutionRules: Rule[SparkPlan] =
    CarbonPrivCheck(sparkSession, catalog, aclInterface)

  override def getPreOptimizerRules: Seq[Rule[LogicalPlan]] = {
    super.getPreOptimizerRules ++ Seq(new CarbonPreOptimizerRule)
  }

  /**
    * Internal catalog for managing table and database states.
    */
  /**
    * Create a [CarbonSessionCatalogBuild].
    */
  override protected lazy val catalog: CarbonACLSessionCatalog = {
    val catalog = new CarbonACLSessionCatalog(
      externalCatalog,
      session.sharedState.globalTempViewManager,
      functionRegistry,
      sparkSession,
      conf,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf),
      sqlParser,
      resourceLoader
    )
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  private def externalCatalog: HiveACLExternalCatalog =
    session.sharedState.externalCatalog.asInstanceOf[HiveACLExternalCatalog]

  //override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)

  override protected def analyzer: Analyzer = new CarbonAnalyzer(catalog, conf, sparkSession,
    new Analyzer(catalog, conf) {

      override val extendedResolutionRules: Seq[Rule[LogicalPlan]] =
        new CarbonAccessControlRules(sparkSession, catalog, aclInterface) +:
        new ResolveHiveSerdeTable(session) +:
          new FindDataSourceTable(session) +:
          new ResolveSQLOnFile(session) +:
          new CarbonIUDAnalysisRule(sparkSession) +:
          new CarbonPreInsertionCasts(sparkSession) +: customResolutionRules

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

  // initialize all listeners
  CarbonACLInternalSessionStateBuilder.init(sparkSession)

  class CarbonPreOptimizerRule extends Rule[LogicalPlan] {

    override def apply(plan: LogicalPlan): LogicalPlan = {
      CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    }
  }

  override def build(): SessionState = {
    val state = super.build()
    state.preExecutionRules = state.preExecutionRules :+ preExecutionRules
    catalog.setHiveClient(client)
    catalog.setACLInterface(aclInterface)
    state
  }

  override protected def newBuilder: NewBuilder = new CarbonACLInternalSessionStateBuilder(_, _)
}

// Register all the required listeners using the singleton instance as the listeners
// need to be registered only once
object CarbonACLInternalSessionStateBuilder {
  var initialized = false

  def init(sparkSession: SparkSession): Unit = {
    if (!initialized) {
      CarbonCommonInitializer.init(sparkSession)
      initialized = true
    }
  }
}

class CarbonInternalSqlAstBuilder(conf: SQLConf, parser: CarbonInternalSpark2SqlParser,
  sparkSession: SparkSession) extends CarbonACLSqlAstBuilder(conf, parser, sparkSession) {

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }
}

class CarbonACLSqlAstBuilder(conf: SQLConf, parser: CarbonSpark2SqlParser,
  sparkSession: SparkSession) extends SparkACLSqlAstBuilder(conf) {

  val helper = new CarbonHelperACLSqlAstBuilder(conf, parser, sparkSession)

  override def visitCreateHiveTable(ctx: CreateHiveTableContext): LogicalPlan = {
    val fileStorage = helper.getFileStorage(ctx.createFileFormat)

    if (fileStorage.equalsIgnoreCase("'carbondata'") ||
      fileStorage.equalsIgnoreCase("'org.apache.carbondata.format'")) {
      helper.createCarbonTable(
        tableHeader = ctx.createTableHeader,
        skewSpecContext = ctx.skewSpec,
        bucketSpecContext = ctx.bucketSpec,
        partitionColumns = ctx.partitionColumns,
        columns = ctx.columns,
        tablePropertyList = ctx.tablePropertyList,
        locationSpecContext = ctx.locationSpec(),
        tableComment = Option(ctx.STRING()).map(string),
        ctas = ctx.AS,
        query = ctx.query
      )
    } else {
      if (SparkUtil.isUQuery) {
        helper.validateFileFormat(
          ctx.createTableHeader,
          ctx.locationSpec,
          ctx.createFileFormat
        )
      }
      super.visitCreateHiveTable(ctx)
    }
  }

  override def visitChangeColumn(ctx: ChangeColumnContext): LogicalPlan = {

    val newColumn = visitColType(ctx.colType)
    if (!ctx.identifier.getText.equalsIgnoreCase(newColumn.name)) {
      throw new MalformedCarbonCommandException(
        "Column names provided are different. Both the column names should be same"
      )
    }

    val (typeString, values): (String, Option[List[(Int, Int)]]) = newColumn.dataType match {
      case d: DecimalType => ("decimal", Some(List((d.precision, d.scale))))
      case _ => (newColumn.dataType.typeName.toLowerCase, None)
    }

    val alterTableChangeDataTypeModel =
      AlterTableDataTypeChangeModel(new CarbonSpark2SqlParser().parseDataType(typeString, values),
        new CarbonSpark2SqlParser()
          .convertDbNameToLowerCase(Option(ctx.tableIdentifier().db).map(_.getText)),
        ctx.tableIdentifier().table.getText.toLowerCase,
        ctx.identifier.getText.toLowerCase,
        newColumn.name.toLowerCase
      )

    CarbonAlterTableDataTypeChangeCommand(alterTableChangeDataTypeModel)
  }


  override def visitAddTableColumns(ctx: AddTableColumnsContext): LogicalPlan = {

    val cols = Option(ctx.columns).toSeq.flatMap(visitColTypeList)
    val fields = parser.getFields(cols)
    val tblProperties = scala.collection.mutable.Map.empty[String, String]
    val tableModel = new CarbonSpark2SqlParser().prepareTableModel(false,
      new CarbonSpark2SqlParser().convertDbNameToLowerCase(Option(ctx.tableIdentifier().db)
        .map(_.getText)
      ),
      ctx.tableIdentifier.table.getText.toLowerCase,
      fields,
      Seq.empty,
      tblProperties,
      None,
      true
    )

    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      Option(ctx.tableIdentifier().db).map(_.getText),
      ctx.tableIdentifier.table.getText,
      tblProperties.toMap,
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highcardinalitydims.getOrElse(Seq.empty)
    )

    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  override def visitCreateTable(ctx: CreateTableContext): LogicalPlan = {
    super.visitCreateTable(ctx)
  }

  override def visitShowTables(ctx: ShowTablesContext): LogicalPlan = withOrigin(ctx) {
    CarbonShowTablesCommand(
      Option(ctx.db).map(_.getText),
      Option(ctx.pattern).map(string)
    )
  }
}
