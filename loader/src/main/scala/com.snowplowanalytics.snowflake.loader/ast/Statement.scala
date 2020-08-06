/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.loader.ast

import cats.implicits._
import com.snowplowanalytics.snowflake.loader.ast.CopyInto._

trait Statement[-S] {
  def getStatement(ast: S): Statement.SqlStatement
}

object Statement {

  final case class SqlStatement private(value: String) extends AnyVal

  implicit object CreateTableStatement extends Statement[CreateTable] {
    def getStatement(ddl: CreateTable): SqlStatement = {
      val constraint = ddl.primaryKey.map { p => ", " + p.show }.getOrElse("")
      val cols = ddl.columns.map(_.show).map(_.split(" ").toList).map {
        case columnName :: tail => columnName + " " + tail.mkString(" ")
        case other => other.mkString(" ")
      }
      val temporary = if (ddl.temporary) " TEMPORARY " else " "
      SqlStatement(s"CREATE${temporary}TABLE IF NOT EXISTS ${ddl.schema}.${ddl.name} (" +
        cols.mkString(", ") + constraint + ")"
      )
    }
  }

  implicit object CreateSchemaStatement extends Statement[CreateSchema] {
    def getStatement(ddl: CreateSchema): SqlStatement =
      SqlStatement(s"CREATE SCHEMA IF NOT EXISTS ${ddl.name}")
  }

  implicit object CreateFileFormatStatement extends Statement[CreateFileFormat] {
    def getStatement(ddl: CreateFileFormat): SqlStatement = ddl match {
      case CreateFileFormat.CreateCsvFormat(name, recordDelimiter, fieldDelimiter) =>
        val recordDelRendered = s"RECORD_DELIMITER = '${recordDelimiter.getOrElse("NONE")}'"
        val fieldDelRendered = s"FIELD_DELIMITER = '${fieldDelimiter.getOrElse("NONE")}'"
        SqlStatement(s"CREATE FILE FORMAT IF NOT EXISTS $name TYPE = CSV $recordDelRendered $fieldDelRendered")
      case CreateFileFormat.CreateJsonFormat(name) =>
        SqlStatement(s"CREATE FILE FORMAT IF NOT EXISTS $name TYPE = JSON")
    }
  }

  implicit object CreateStageStatement extends Statement[CreateStage] {
    def getStatement(ddl: CreateStage): SqlStatement = {
      val auth = ddl.credentials.fold(""){
        case Common.AwsKeys(accessKey, secretKey, token) =>
          if (token.isDefined) {
            System.err.println("AWS_TOKEN (temporary credentials) must never be used for stages. Skipping credentials")
            ""
          } else s" CREDENTIALS = ( AWS_KEY_ID = '$accessKey' AWS_SECRET_KEY = '$secretKey' )"
        case Common.AwsRole(arn) => s" CREDENTIALS = ( AWS_ROLE = '$arn' )"
        case Common.StorageIntegration(name) => s" STORAGE_INTEGRATION = $name"
      }
      SqlStatement(
        s"CREATE STAGE IF NOT EXISTS ${ddl.schema}.${ddl.name} URL = '${ddl.url}' FILE_FORMAT = ${ddl.fileFormat}$auth"
      )
    }
  }

  implicit object CreateWarehouseStatement extends Statement[CreateWarehouse] {
    def getStatement(ddl: CreateWarehouse): SqlStatement = {
      val size = ddl.size.getOrElse(CreateWarehouse.DefaultSize).toString.toUpperCase
      val autoSuspend = ddl.autoSuspend.getOrElse(CreateWarehouse.DefaultAutoSuspend).toString
      val autoResume = ddl.autoResume.getOrElse(CreateWarehouse.DefaultAutoResume).toString.toUpperCase
      SqlStatement(s"CREATE WAREHOUSE IF NOT EXISTS ${ddl.name} WAREHOUSE_SIZE = $size AUTO_SUSPEND = $autoSuspend AUTO_RESUME = $autoResume")
    }
  }

  implicit object SelectStatement extends Statement[Select] {
    def getStatement(ddl: Select): SqlStatement =
      SqlStatement(s"SELECT ${ddl.columns.map(_.show).mkString(", ")} FROM ${ddl.schema}.${ddl.table}")
  }

  implicit object InsertStatement extends Statement[Insert] {
    def getStatement(ddl: Insert): SqlStatement = ddl match {
      case Insert.InsertQuery(schema, table, columns, from) =>
        SqlStatement(s"INSERT INTO $schema.$table(${columns.mkString(",")}) ${from.getStatement.value}")
    }
  }

  implicit object AlterTableStatement extends Statement[AlterTable] {
    def getStatement(ast: AlterTable): SqlStatement = ast match {
      case AlterTable.AddColumn(schema, table, column, datatype) =>
        SqlStatement(s"ALTER TABLE $schema.$table ADD COLUMN $column ${datatype.show}")
      case AlterTable.DropColumn(schema, table, column) =>
        SqlStatement(s"ALTER TABLE $schema.$table DROP COLUMN $column")
      case AlterTable.AlterColumnDatatype(schema, table, column, datatype) =>
        SqlStatement(s"ALTER TABLE $schema.$table ALTER COLUMN $column SET DATA TYPE ${datatype.show}")
    }
  }

  implicit object AlterWarehouseStatement extends Statement[AlterWarehouse] {
    def getStatement(ast: AlterWarehouse): SqlStatement = ast match {
      case AlterWarehouse.Resume(warehouse) =>
        SqlStatement(s"ALTER WAREHOUSE $warehouse RESUME")
    }
  }

  implicit object UseWarehouseStatement extends Statement[UseWarehouse] {
    def getStatement(ast: UseWarehouse): SqlStatement =
      SqlStatement(s"USE WAREHOUSE ${ast.warehouse}")
  }

  implicit object CopyInto extends Statement[CopyInto] {
    def getStatement(ast: CopyInto): SqlStatement = {
      // COPY INTO TABLE supports IAM keys and storage option for authentication only
      val auth = ast.credentials.fold(""){
        case Common.AwsKeys(accessKey, secretKey, token) =>
          val preparedToken = token.fold("")(t => s" AWS_TOKEN = '$t'")
          s" CREDENTIALS = ( AWS_KEY_ID = '$accessKey' AWS_SECRET_KEY = '$secretKey'$preparedToken )"
        case Common.StorageIntegration(name) => s" STORAGE_INTEGRATION = $name"
        case _ => ""
      }
      val onError = ast.onError match {
        case Some(Continue) => s"CONTINUE"
        case Some(SkipFile) => s"SKIP_FILE"
        case Some(SkipFileNum(value)) => s"SKIP_FILE_$value"
        case Some(SkipFilePercentage(value)) => s"SKIP_FILE_$value%"
        case Some(AbortStatement) => s"ABORT_STATEMENT"
        case None => ""
      }
      val copyOptions = if (onError == "") "" else s" ON_ERROR = $onError"
      val stripNulls = if (ast.stripNullValues) " STRIP_NULL_VALUES = TRUE" else ""
      SqlStatement(s"COPY INTO ${ast.schema}.${ast.table}(${ast.columns.mkString(",")}) FROM @${ast.from.schema}.${ast.from.stageName}/${ast.from.path}$auth$copyOptions FILE_FORMAT = (FORMAT_NAME = '${ast.fileFormat.schema}.${ast.fileFormat.formatName}'$stripNulls)" )
    }
  }

  implicit object ShowStageStatement extends Statement[Show.ShowStages] {
    def getStatement(ast: Show.ShowStages): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW stages$schemaPattern$scopePattern")
    }
  }

  implicit object ShowSchemasStatement extends Statement[Show.ShowSchemas] {
    def getStatement(ast: Show.ShowSchemas): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      SqlStatement(s"SHOW schemas$schemaPattern")
    }
  }

  implicit object ShowTablesStatement extends Statement[Show.ShowTables] {
    def getStatement(ast: Show.ShowTables): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW tables$schemaPattern$scopePattern")
    }
  }

  implicit object ShowFileFormatsStatement extends Statement[Show.ShowFileFormats] {
    def getStatement(ast: Show.ShowFileFormats): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      val scopePattern = ast.schema.map(s => s" IN $s").getOrElse("")
      SqlStatement(s"SHOW file formats$schemaPattern$scopePattern")
    }
  }

  implicit object ShowWarehousesStatement extends Statement[Show.ShowWarehouses] {
    def getStatement(ast: Show.ShowWarehouses): SqlStatement = {
      val schemaPattern = ast.pattern.map(s => s" LIKE '$s'").getOrElse("")
      SqlStatement(s"SHOW warehouses$schemaPattern")
    }
  }
}
