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

import cats.syntax.either._

sealed trait SnowflakeDatatype extends Product with Serializable

object SnowflakeDatatype {
  final case class Varchar(size: Option[Int]) extends SnowflakeDatatype
  final case object Timestamp extends SnowflakeDatatype
  final case class Char(size: Int) extends SnowflakeDatatype
  final case object SmallInt extends SnowflakeDatatype
  final case object DoublePrecision extends SnowflakeDatatype
  final case object Integer extends SnowflakeDatatype
  final case class Number(precision: Int, scale: Int) extends SnowflakeDatatype
  final case object Boolean extends SnowflakeDatatype
  final case object Variant extends SnowflakeDatatype
  final case object JsonObject extends SnowflakeDatatype
  final case object JsonArray extends SnowflakeDatatype

  private val varcharRegex = """VARCHAR\((\d+)\)""".r
  private val charRegex = """CHAR\((\d+)\)""".r
  private val timestampRegex = """TIMESTAMP_NTZ\((\d+)\)""".r
  private val numberRegex = """NUMBER\((\d+),(\d+)\)""".r

  def parse(s: String): Either[String, SnowflakeDatatype] =
    s match {
      case varcharRegex(size) =>
        Either.catchOnly[NumberFormatException](size.toInt)
          .leftMap(_.getMessage)
          .map(size => Varchar(Some(size)))
      case "VARCHAR" => Varchar(None).asRight
      case charRegex(size) =>
        Either.catchOnly[NumberFormatException](size.toInt)
          .leftMap(_.getMessage)
          .map(size => Char(size))
      case timestampRegex(_) =>
        Timestamp.asRight
      case numberRegex("38", "0") | "INTEGER" | "INT" =>
        Integer.asRight
      case numberRegex(precision, scale) =>
        for {
          precision <- Either.catchOnly[NumberFormatException](precision.toInt).leftMap(_.getMessage)
          scale <- Either.catchOnly[NumberFormatException](scale.toInt).leftMap(_.getMessage)
        } yield Number(precision, scale)
      case "FLOAT" | "DOUBLE PRECISION" | "DOUBLE" | "REAL" =>
        DoublePrecision.asRight
      case "BOOLEAN" =>
        Boolean.asRight
      case "ARRAY" =>
        JsonArray.asRight
      case "OBJECT" =>
        JsonObject.asRight
      case "VARIANT" =>
        Variant.asRight
      case "SMALLINT" =>  // Represented as NUMBER(38,0)
        SmallInt.asRight
      case _ => s"Unknown type $s".asLeft
    }
}
