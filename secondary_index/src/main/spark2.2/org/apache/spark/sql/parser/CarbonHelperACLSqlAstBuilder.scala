package org.apache.spark.sql.parser

import scala.collection.mutable

import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.parser.{ParseException, SqlBaseParser}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableAsSelectCommand, CarbonCreateTableCommand}
import org.apache.spark.sql.execution.command.{PartitionerField, TableModel, TableNewProcessor}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{CarbonEnv, SparkACLSqlAstBuilder, SparkSession}
import org.apache.spark.util.SparkUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.spark.CarbonOption
import org.apache.carbondata.spark.util.CommonUtil

class CarbonHelperACLSqlAstBuilder(conf: SQLConf,
  parser: CarbonSpark2SqlParser,
  sparkSession: SparkSession)
  extends SparkACLSqlAstBuilder(conf) {

  // For uquery, if it is external table, only parquet or orc is allowed
  def validateFileFormat(
    tableHeader: CreateTableHeaderContext,
    locationSpec: LocationSpecContext,
    createFileFormat: CreateFileFormatContext): Unit = {
    val (_, _, _, external) = visitCreateTableHeader(tableHeader)
    val location = Option(locationSpec).map(visitLocationSpec)
    Option(createFileFormat).foreach { ctx =>
      (ctx.fileFormat, ctx.storageHandler) match {
        // STORED AS
        case (c: GenericFileFormatContext, null) =>
          val source = c.identifier.getText
          // If location is defined, we'll assume this is an external table.
          if (external || location.isDefined) {
            if (!source.equalsIgnoreCase("parquet") && !source.equalsIgnoreCase("orc")) {
              operationNotAllowed(
                "Only support 'STORED AS' PARQUET/ORC for external table.", ctx
              )
            }
          } else {
            operationNotAllowed(
              "'STORED AS' FileFormat is not supported for internal table.", ctx
            )
          }
        // STORED BY
        case (null, s: StorageHandlerContext) =>
          operationNotAllowed(
            "'STORED BY' StorageHandler is not supported for internal table.", ctx
          )
        case _ =>
      }
    }

  }

  def getFileStorage(createFileFormat: CreateFileFormatContext): String = {
    Option(createFileFormat) match {
      case Some(value) =>
        if (value.children.get(1).getText.equalsIgnoreCase("by")) {
          value.storageHandler().STRING().getSymbol.getText
        } else {
          // The case of "STORED AS PARQUET/ORC"
          ""
        }
      case _ => ""
    }
  }

  /**
    * This method will convert the database name to lower case
    *
    * @param dbName
    * @return Option of String
    */
  def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(databaseName.toLowerCase)
      case None => dbName
    }
  }


  def needToConvertToLowerCase(key: String): Boolean = {
    val noConvertList = Array("LIST_INFO", "RANGE_INFO")
    !noConvertList.exists(x => x.equalsIgnoreCase(key));
  }

  /**
    * Parse a key-value map from a [[TablePropertyListContext]], assuming all values are specified.
    */
  def visitPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String] = {
    val props = visitTablePropertyList(ctx)
    val badKeys = props.filter { case (_, v) => v == null }.keys
    if (badKeys.nonEmpty) {
      operationNotAllowed(
        s"Values must be specified for key(s): ${badKeys.mkString("[", ",", "]")}", ctx
      )
    }
    props.map { case (key, value) =>
      if (needToConvertToLowerCase(key)) {
        (key.toLowerCase, value.toLowerCase)
      } else {
        (key.toLowerCase, value)
      }
    }
  }

  def getPropertyKeyValues(ctx: TablePropertyListContext): Map[String, String]
  = {
    Option(ctx).map(visitPropertyKeyValues)
      .getOrElse(Map.empty)
  }

  def createCarbonTable(tableHeader: CreateTableHeaderContext,
    skewSpecContext: SkewSpecContext,
    bucketSpecContext: BucketSpecContext,
    partitionColumns: ColTypeListContext,
    columns: ColTypeListContext,
    tablePropertyList: TablePropertyListContext,
    locationSpecContext: SqlBaseParser.LocationSpecContext,
    tableComment: Option[String],
    ctas: TerminalNode,
    query: QueryContext): LogicalPlan = {

    val (tableIdentifier, temp, ifNotExists, external) = visitCreateTableHeader(tableHeader)
    // TODO: implement temporary tables
    if (temp) {
      throw new ParseException(
        "CREATE TEMPORARY TABLE is not supported yet. " +
          "Please use CREATE TEMPORARY VIEW as an alternative.", tableHeader
      )
    }
    if (skewSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... SKEWED BY", skewSpecContext)
    }
    if (bucketSpecContext != null) {
      operationNotAllowed("CREATE TABLE ... CLUSTERED BY", bucketSpecContext)
    }
    if (external) {
      operationNotAllowed("CREATE EXTERNAL TABLE", tableHeader)
    }

    val cols = Option(columns).toSeq.flatMap(visitColTypeList)

    // Complex type is not supported in UQuery
    if (SparkUtil.isUQuery) {
      cols.foreach { column =>
        val typeName = column.dataType.typeName
        if (typeName.equalsIgnoreCase("array") || typeName.equalsIgnoreCase("struct") ||
          typeName.equalsIgnoreCase("map")) {
          operationNotAllowed("Unsupported data type: complex data type", columns)
        }
      }
    }

    // Ensuring whether no duplicate name is used in table definition
    val colNames = cols.map(_.name)
    if (colNames.length != colNames.distinct.length) {
      val duplicateColumns = colNames.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => "\"" + x + "\""
      }
      operationNotAllowed(s"Duplicated column names found in table definition of " +
        s"$tableIdentifier: ${duplicateColumns.mkString("[", ",", "]")}", columns
      )
    }

    val tablePath = if (locationSpecContext != null) {
      Some(visitLocationSpec(locationSpecContext))
    } else {
      None
    }

    val tableProperties = mutable.Map[String, String]()
    val properties = getPropertyKeyValues(tablePropertyList)
    properties.foreach { property => tableProperties.put(property._1, property._2) }

    // validate partition clause
    val (partitionByStructFields, partitionFields) =
      validateParitionFields(partitionColumns, colNames, tableProperties)

    // validate partition clause
    if (partitionFields.nonEmpty) {
      if (!CommonUtil.validatePartitionColumns(tableProperties, partitionFields)) {
        throw new MalformedCarbonCommandException("Error: Invalid partition definition")
      }
      // partition columns should not be part of the schema
      val badPartCols = partitionFields
        .map(_.partitionColumn.toLowerCase)
        .toSet
        .intersect(colNames.map(_.toLowerCase).toSet)

      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
          badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]"),
          partitionColumns
        )
      }
    }

    val options = new CarbonOption(properties)
    // validate streaming property
    validateStreamingProperty(options)
    var fields = parser.getFields(cols ++ partitionByStructFields)
    // validate for create table as select
    val selectQuery = Option(query).map(plan)
    selectQuery match {
      case Some(q) =>
        // create table as select does not allow creation of partitioned table
        if (partitionFields.nonEmpty) {
          val errorMessage = "A Create Table As Select (CTAS) statement is not allowed to " +
            "create a partitioned table using Carbondata file formats."
          operationNotAllowed(errorMessage, partitionColumns)
        }
        // create table as select does not allow to explicitly specify schema
        if (fields.nonEmpty) {
          operationNotAllowed(
            "Schema may not be specified in a Create Table As Select (CTAS) statement", columns
          )
        }
        fields = parser
          .getFields(CarbonEnv.getInstance(sparkSession).carbonMetastore
            .getSchemaFromUnresolvedRelation(sparkSession, Some(q).get)
          )
      case _ =>
      // ignore this case
    }
    if (partitionFields.nonEmpty && options.isStreaming) {
      operationNotAllowed("Streaming is not allowed on partitioned table", partitionColumns)
    }
    // validate tblProperties
    val bucketFields = parser.getBucketFields(tableProperties, fields, options)
    // prepare table model of the collected tokens
    val tableModel: TableModel = parser.prepareTableModel(
      ifNotExists,
      convertDbNameToLowerCase(tableIdentifier.database),
      tableIdentifier.table.toLowerCase,
      fields,
      partitionFields,
      tableProperties,
      bucketFields,
      isAlterFlow = false,
      tableComment
    )

    selectQuery match {
      case query@Some(q) =>
        CarbonCreateTableAsSelectCommand(
          TableNewProcessor(tableModel),
          query.get,
          tableModel.ifNotExistsSet,
          tablePath
        )
      case _ =>
        CarbonCreateTableCommand(TableNewProcessor(tableModel),
          tableModel.ifNotExistsSet,
          tablePath
        )
    }
  }

  private def validateStreamingProperty(carbonOption: CarbonOption): Unit = {
    try {
      carbonOption.isStreaming
    } catch {
      case _: IllegalArgumentException =>
        throw new MalformedCarbonCommandException(
          "Table property 'streaming' should be either 'true' or 'false'"
        )
    }
  }

  private def validateParitionFields(
    partitionColumns: ColTypeListContext,
    colNames: Seq[String],
    tableProperties: mutable.Map[String, String]): (Seq[StructField], Seq[PartitionerField]) = {
    val partitionByStructFields = Option(partitionColumns).toSeq.flatMap(visitColTypeList)
    val partitionerFields = partitionByStructFields.map { structField =>
      PartitionerField(structField.name, Some(structField.dataType.toString), null)
    }
    if (partitionerFields.nonEmpty) {
      if (!CommonUtil.validatePartitionColumns(tableProperties, partitionerFields)) {
        throw new MalformedCarbonCommandException("Error: Invalid partition definition")
      }
      // partition columns should not be part of the schema
      val badPartCols = partitionerFields.map(_.partitionColumn).toSet.intersect(colNames.toSet)
      if (badPartCols.nonEmpty) {
        operationNotAllowed(s"Partition columns should not be specified in the schema: " +
          badPartCols.map("\"" + _ + "\"").mkString("[", ",", "]")
          , partitionColumns: ColTypeListContext
        )
      }
    }
    (partitionByStructFields, partitionerFields)
  }

}

