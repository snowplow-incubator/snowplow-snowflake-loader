/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowflake.loader

import cats.Show
import cats.implicits._
import ast.CreateTable._
import ast.SnowflakeDatatype._
import ast.Select.Substring

package object ast {
  implicit object DatatypeShow extends Show[SnowflakeDatatype] {
    def show(ddl: SnowflakeDatatype): String = ddl match {
      case Varchar(Some(size))      => s"VARCHAR($size)"
      case Varchar(None)            => s"VARCHAR"
      case Timestamp                => "TIMESTAMP"
      case Char(size)               => s"CHAR($size)"
      case SmallInt                 => "SMALLINT"
      case DoublePrecision          => "DOUBLE PRECISION"
      case Integer                  => "INTEGER"
      case Number(precision, scale) => s"NUMBER($precision,$scale)"
      case Boolean                  => "BOOLEAN"
      case Variant                  => "VARIANT"
      case JsonObject               => "OBJECT"
      case JsonArray                => "ARRAY"
    }
  }

  implicit object PrimaryKeyShow extends Show[PrimaryKeyConstraint] {
    def show(ddl: PrimaryKeyConstraint): String =
      s"CONSTRAINT ${ddl.name} PRIMARY KEY(${ddl.column})"
  }

  implicit object ColumnShow extends Show[Column] {
    def show(ddl: Column): String = {
      val datatype = ddl.dataType.show
      val constraints = ((if (ddl.notNull) "NOT NULL" else "") :: (if (ddl.unique) "UNIQUE" else "") :: Nil).filterNot(_.isEmpty)
      val renderedConstraints = if (constraints.isEmpty) "" else " " + constraints.mkString(" ")
      s"${ddl.name} $datatype" + renderedConstraints
    }
  }

  implicit object CastedColumnShow extends Show[Select.CastedColumn] {
    def show(column: Select.CastedColumn): String = {
      column.substring match {
        case Some(Substring(start, length)) =>
          val columnName = s"${column.originColumn}:${column.columnName}"
          s"substr($columnName,$start,$length)::${column.datatype.show}"
        case None =>
          s"${column.originColumn}:${column.columnName}::${column.datatype.show}"
      }
    }
  }

  implicit class StatementSyntax[S](val ast: S) extends AnyVal {
    def getStatement(implicit S: Statement[S]): Statement.SqlStatement =
      S.getStatement(ast)
  }
}
