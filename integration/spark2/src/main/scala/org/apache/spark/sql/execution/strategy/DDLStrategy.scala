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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand, CarbonLoadDataCommand, RefreshCarbonTableCommand}
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand, CarbonAlterTableDropPartitionCommand, CarbonShowCarbonPartitionsCommand}
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonCreateDataSourceTableCommand, CarbonDescribeFormattedCommand, CarbonDropTableCommand}
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand, MatchResetCommand}
import org.apache.spark.sql.CarbonExpressions.{CarbonDescribeTable => DescribeTableCommand}
import org.apache.spark.sql.execution.datasources.{RefreshResource, RefreshTable}
import org.apache.spark.sql.hive.{CarbonRelation, CreateCarbonSourceTableAsSelectCommand}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.{CarbonReflectionUtils, DataMapUtil, FileUtils, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.{CarbonProperties, DataTypeUtil, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.util.{DataTypeConverterUtil, Util}

  /**
   * Carbon strategies for ddl commands
   * CreateDataSourceTableAsSelectCommand class has extra argument in
   * 2.3, so need to add wrapper to match the case
   */
object MatchCreateDataSourceTable {
  def unapply(plan: LogicalPlan): Option[(CatalogTable, SaveMode, LogicalPlan)] = plan match {
    case t: CreateDataSourceTableAsSelectCommand => Some(t.table, t.mode, t.query)
    case _ => None
  }
}

class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case LoadDataCommand(identifier, path, isLocal, isOverwrite, partition)
        if isCarbonTable(identifier) =>
        ExecutedCommandExec(
          CarbonLoadDataCommand(
            databaseNameOp = identifier.database,
            tableName = identifier.table.toLowerCase,
            factPathFromUser = path,
            dimFilesPath = Seq(),
            options = Map(),
            isOverwriteTable = isOverwrite,
            inputSqlString = null,
            dataFrame = None,
            updateModel = None,
            tableInfoOp = None,
            internalOptions = Map.empty,
            partition = partition.getOrElse(Map.empty).map { case (col, value) =>
              (col, Some(value))})) :: Nil
      case AlterTableRenameCommand(oldTableIdentifier, newTableIdentifier, _)
        if isCarbonTable(oldTableIdentifier) =>
        val dbOption = oldTableIdentifier.database.map(_.toLowerCase)
        val tableIdentifier = TableIdentifier(oldTableIdentifier.table.toLowerCase(), dbOption)
        val renameModel = AlterTableRenameModel(tableIdentifier, newTableIdentifier)
        ExecutedCommandExec(CarbonAlterTableRenameCommand(renameModel)) :: Nil
      case DropTableCommand(identifier, ifNotExists, isView, _)
        if isCarbonTable(identifier) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database,
            identifier.table.toLowerCase)) :: Nil
      case createLikeTable: CreateTableLikeCommand if isCarbonTable(createLikeTable.sourceTable)=>
        throw new MalformedCarbonCommandException(
          "Operation not allowed, when source table is carbon table")
      case InsertIntoCarbonTable(relation: CarbonDatasourceHadoopRelation,
      partition, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(CarbonInsertIntoCommand(relation, child, overwrite, partition)) :: Nil
      case createDb@CreateDatabaseCommand(dbName, ifNotExists, _, _, _) =>
        val dbLocation = try {
          CarbonEnv.getDatabaseLocation(dbName, sparkSession)
        } catch {
          case e: NoSuchDatabaseException =>
            CarbonProperties.getStorePath
        }
        ThreadLocalSessionInfo
          .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
        FileUtils.createDatabaseDirectory(dbName, dbLocation, sparkSession.sparkContext)
        ExecutedCommandExec(createDb) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, isCascade)
        if CarbonEnv.databaseLocationExists(dbName, sparkSession, ifExists) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      case alterTable@CarbonAlterTableCompactionCommand(altertablemodel, _, _) =>
        if (isCarbonTable(TableIdentifier(altertablemodel.tableName, altertablemodel.dbName))) {
            ExecutedCommandExec(alterTable) :: Nil
        } else {
          throw new MalformedCarbonCommandException(
            String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
            altertablemodel.tableName,
            altertablemodel.dbName.getOrElse("default")))
        }
      case AlterTableChangeColumnCommand(tableName, columnName, newColumn)
        if isCarbonTable(tableName) =>
        var isColumnRename = false
        // If both the column name are not same, then its a call for column rename
        if (!columnName.equalsIgnoreCase(newColumn.name)) {
          isColumnRename = true
        }
        val alterTableColRenameAndDataTypeChangeModel =
          AlterTableDataTypeChangeModel(
            DataTypeInfo(
              DataTypeConverterUtil
                .convertToCarbonType(newColumn.dataType.typeName)
                .getName
                .toLowerCase
            ),
            tableName.database.map(_.toLowerCase),
            tableName.table.toLowerCase,
            columnName.toLowerCase,
            newColumn.name.toLowerCase,
            isColumnRename)
        ExecutedCommandExec(
          CarbonAlterTableColRenameDataTypeChangeCommand(
            alterTableColRenameAndDataTypeChangeModel
          )
        ) :: Nil
      case colRenameDataTypeChange@CarbonAlterTableColRenameDataTypeChangeCommand(
      alterTableColRenameAndDataTypeChangeModel, _) =>
        if (isCarbonTable(TableIdentifier(
          alterTableColRenameAndDataTypeChangeModel.tableName,
          alterTableColRenameAndDataTypeChangeModel.databaseName))) {
          val carbonTable = CarbonEnv
            .getCarbonTable(alterTableColRenameAndDataTypeChangeModel.databaseName,
              alterTableColRenameAndDataTypeChangeModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(colRenameDataTypeChange) :: Nil
          }
        } else {
          throw new MalformedCarbonCommandException(
            String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
              alterTableColRenameAndDataTypeChangeModel.tableName,
              alterTableColRenameAndDataTypeChangeModel.
                databaseName.getOrElse("default")))
        }
      case AlterTableAddColumnsCommand(table, colsToAdd) if isCarbonTable(table) =>
        val fields = new CarbonSpark2SqlParser().getFields(colsToAdd)
        val tableModel = CarbonParserUtil.prepareTableModel (false,
          CarbonParserUtil.convertDbNameToLowerCase(table.database),
          table.table.toLowerCase,
          fields.map(CarbonParserUtil.convertFieldNamesToLowercase),
          Seq.empty,
          scala.collection.mutable.Map.empty[String, String],
          None,
          true)

        val alterTableAddColumnsModel = AlterTableAddColumnsModel(
          CarbonParserUtil.convertDbNameToLowerCase(table.database),
          table.table.toLowerCase,
          Map.empty[String, String],
          tableModel.dimCols,
          tableModel.msrCols,
          tableModel.highcardinalitydims.getOrElse(Seq.empty))
        ExecutedCommandExec(CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)):: Nil
      case addColumn@CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel) =>
        if (isCarbonTable(TableIdentifier(
          alterTableAddColumnsModel.tableName,
          alterTableAddColumnsModel.databaseName))) {
          val carbonTable = CarbonEnv.getCarbonTable(alterTableAddColumnsModel.databaseName,
            alterTableAddColumnsModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(addColumn) :: Nil
          }
          // TODO: remove this else if check once the 2.1 version is unsupported by carbon
        } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
          val structField = (alterTableAddColumnsModel.dimCols ++ alterTableAddColumnsModel.msrCols)
            .map {
              a =>
                StructField(a.column,
                  Util.convertCarbonToSparkDataType(DataTypeUtil.valueOf(a.dataType.get)))
            }
          val identifier = TableIdentifier(
            alterTableAddColumnsModel.tableName,
            alterTableAddColumnsModel.databaseName)
          ExecutedCommandExec(CarbonReflectionUtils
            .invokeAlterTableAddColumn(identifier, structField).asInstanceOf[RunnableCommand]) ::
          Nil
          // TODO: remove this else check once the 2.1 version is unsupported by carbon
        } else {
          throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
        }
      case dropColumn@CarbonAlterTableDropColumnCommand(alterTableDropColumnModel)
        if isCarbonTable(TableIdentifier(alterTableDropColumnModel.tableName,
          alterTableDropColumnModel.databaseName)) =>
          val carbonTable = CarbonEnv.getCarbonTable(alterTableDropColumnModel.databaseName,
            alterTableDropColumnModel.tableName)(sparkSession)
          if (carbonTable != null && carbonTable.isFileLevelFormat) {
            throw new MalformedCarbonCommandException(
              "Unsupported alter operation on Carbon external fileformat table")
          } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
            throw new MalformedCarbonCommandException(
              "Unsupported operation on non transactional table")
          } else {
            ExecutedCommandExec(dropColumn) :: Nil
          }
      case desc@DescribeTableCommand(identifier, partitionSpec, isExtended) =>
        val isFormatted: Boolean = if (sparkSession.version.startsWith("2.1")) {
          CarbonReflectionUtils
            .getDescribeTableFormattedField(desc.asInstanceOf[DescribeTableCommand])
        } else {
          false
        }
        if (isCarbonTable(identifier) && (isExtended || isFormatted)) {
          val resolvedTable =
            sparkSession.sessionState.executePlan(UnresolvedRelation(identifier)).analyzed
          val resultPlan = sparkSession.sessionState.executePlan(resolvedTable).executedPlan
          ExecutedCommandExec(
            CarbonDescribeFormattedCommand(
              resultPlan,
              plan.output,
              partitionSpec,
              identifier)) :: Nil
        } else {
          Nil
        }
      case ShowPartitionsCommand(tableName, cols) if isCarbonTable(tableName) =>
        val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(tableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
        if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
          throw new MalformedCarbonCommandException(
            "Unsupported operation on non transactional table")
        }
        if (!carbonTable.isHivePartitionTable) {
          ExecutedCommandExec(CarbonShowCarbonPartitionsCommand(tableName)) :: Nil
        } else {
          ExecutedCommandExec(ShowPartitionsCommand(tableName, cols)) :: Nil
        }
      case adp@AlterTableDropPartitionCommand(tableName, specs, ifExists, purge, retainData)
        if isCarbonTable(tableName) =>
        ExecutedCommandExec(
          CarbonAlterTableDropHivePartitionCommand(
            tableName,
            specs,
            ifExists,
            purge,
            retainData)) :: Nil
      case set@SetCommand(kv) =>
        ExecutedCommandExec(CarbonSetCommand(set)) :: Nil
      case MatchResetCommand(_) =>
        ExecutedCommandExec(CarbonResetCommand()) :: Nil
      case org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, None)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
          && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog =
          CarbonSource.updateCatalogTableWithCarbonSchema(tableDesc, sparkSession)
        val cmd =
          CreateDataSourceTableCommand(updatedCatalog, ignoreIfExists = mode == SaveMode.Ignore)
        ExecutedCommandExec(cmd) :: Nil
      case MatchCreateDataSourceTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(tableDesc, sparkSession, Option(query))
        val cmd = CreateCarbonSourceTableAsSelectCommand(updatedCatalog, SaveMode.Ignore, query)
        ExecutedCommandExec(cmd) :: Nil
      case cmd@org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, query)
        if tableDesc.provider.get != DDLUtils.HIVE_PROVIDER
           && (tableDesc.provider.get.equals("org.apache.spark.sql.CarbonSource")
               || tableDesc.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(tableDesc, sparkSession, query)
        val cmd = CreateCarbonSourceTableAsSelectCommand(updatedCatalog, SaveMode.Ignore, query.get)
        ExecutedCommandExec(cmd) :: Nil
      case CreateDataSourceTableCommand(table, ignoreIfExists)
        if table.provider.get != DDLUtils.HIVE_PROVIDER
          && (table.provider.get.equals("org.apache.spark.sql.CarbonSource")
          || table.provider.get.equalsIgnoreCase("carbondata")) =>
        val updatedCatalog = CarbonSource
          .updateCatalogTableWithCarbonSchema(table, sparkSession)
        val cmd = new CarbonCreateDataSourceTableCommand(updatedCatalog, ignoreIfExists)
        ExecutedCommandExec(cmd) :: Nil
      case AlterTableSetPropertiesCommand(tableName, properties, isView)
        if isCarbonTable(tableName) => {

        val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .lookupRelation(tableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
        if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
          throw new MalformedCarbonCommandException(
            "Unsupported operation on non transactional table")
        }

        // TODO remove this limitation later
        val property = properties.find(_._1.equalsIgnoreCase("streaming"))
        if (property.isDefined) {
          if (carbonTable.getTablePath.startsWith("s3") && property.get._2.equalsIgnoreCase("s3")) {
            throw new UnsupportedOperationException("streaming is not supported with s3 store")
          }
          if (carbonTable.isStreamingSink) {
            throw new MalformedCarbonCommandException(
              "Streaming property can not be changed once it is 'true'")
          } else {
            if (!property.get._2.trim.equalsIgnoreCase("true")) {
              throw new MalformedCarbonCommandException(
                "Streaming property value is incorrect")
            }
            if (DataMapUtil.hasMVDataMap(carbonTable)) {
              throw new MalformedCarbonCommandException(
                "The table which has MV datamap does not support set streaming property")
            }
            if (carbonTable.isChildTable) {
              throw new MalformedCarbonCommandException(
                "Datamap table does not support set streaming property")
            }
          }
        }
        ExecutedCommandExec(CarbonAlterTableSetCommand(tableName, properties, isView)) :: Nil
      }
      case AlterTableUnsetPropertiesCommand(tableName, propKeys, ifExists, isView)
        if isCarbonTable(tableName) => {
        // TODO remove this limitation later
        if (propKeys.exists(_.equalsIgnoreCase("streaming"))) {
          throw new MalformedCarbonCommandException(
            "Streaming property can not be removed")
        }
        ExecutedCommandExec(
          CarbonAlterTableUnsetCommand(tableName, propKeys, ifExists, isView)) :: Nil
      }
      case AlterTableRenamePartitionCommand(tableName, oldPartition, newPartition)
        if isCarbonTable(tableName) =>
          throw new UnsupportedOperationException("Renaming partition on table is not supported")
      case AlterTableAddPartitionCommand(tableName, partitionSpecsAndLocs, ifNotExists)
        if isCarbonTable(tableName) =>
        ExecutedCommandExec(
          CarbonAlterTableAddHivePartitionCommand(
            tableName,
            partitionSpecsAndLocs,
            ifNotExists)
        ) :: Nil
      case RefreshTable(table) if isCarbonTable(table) =>
        RefreshCarbonTableCommand(table.database,
          table.table).run(sparkSession)
        ExecutedCommandExec(RefreshTable(table)) :: Nil
      case RefreshResource(path : String) =>
        try {
          val plan = new CarbonSpark2SqlParser().parse(s"REFRESH $path")
          ExecutedCommandExec(plan.asInstanceOf[RunnableCommand]) :: Nil
        } catch {
          case e: Exception =>
            LOGGER.error(e.getMessage)
            Nil
        }
      case AlterTableSetLocationCommand(tableName, _, _) if isCarbonTable(tableName) =>
          throw new UnsupportedOperationException("Set partition location is not supported")
      case _ => Nil
    }
  }

  def isCarbonTable(tableIdent: TableIdentifier): Boolean = {
    val dbOption = tableIdent.database.map(_.toLowerCase)
    val tableIdentifier = TableIdentifier(tableIdent.table.toLowerCase(), dbOption)
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .tableExists(tableIdentifier)(sparkSession)
  }

}
