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

import org.specs2.mutable.Specification

import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.loader.ast.Select.Substring

class StatementSpec extends Specification {

  "getStatement" should {
    "Transform CREATE TABLE AST into String" in e1
    "Transform COPY INTO AST (with IAM user) into String" in e2
    "Transform INSERT INTO AST into String" in e3
    "Transform SHOW into String" in e4
    "Transform COPY INTO AST (without credentials) into String" in e5
    "Transform CREATE STAGE AST into String"  in e6
    "Transform COPY INTO AST (with stripping nulls) into String" in e7
    "Transform CREATE WAREHOUSE AST into String" in e8
    "Transform COPY INTO AST (with IAM role) into String" in e9
    "Transform CREATE STAGE (with IAM role) into String" in e10
  }

  def e1 = {
    val columns = List(
      Column("id", SnowflakeDatatype.Number(2, 6), notNull = true),
      Column("foo", SnowflakeDatatype.Varchar(Some(128)), unique = true),
      Column("long_column_name", SnowflakeDatatype.DoublePrecision, unique = true, notNull = true),
      Column("baz", SnowflakeDatatype.Variant))
    val input = CreateTable("nonatomic", "data", columns, None)

    val result = input.getStatement.value
    val expected =
      """CREATE TABLE IF NOT EXISTS nonatomic.data (id NUMBER(2,6) NOT NULL, foo VARCHAR(128) UNIQUE, long_column_name DOUBLE PRECISION NOT NULL UNIQUE, baz VARIANT)"""

    result must beEqualTo(expected)
  }

  def e2 = {
    val columns = List("id", "foo", "fp_id", "json")
    val input = CopyInto(
      "some_schema",
      "some_table",
      columns,
      CopyInto.From("other_schema", "stage_name", "path/to/dir"),
      Some(Common.AwsCreds("AAA", "xyz", None, None)),
      CopyInto.FileFormat("third_schema", "format_name"),
      None,
      false)

    val result = input.getStatement.value
    val expected = "COPY INTO some_schema.some_table(id,foo,fp_id,json) " +
      "FROM @other_schema.stage_name/path/to/dir " +
      "CREDENTIALS = (AWS_KEY_ID = 'AAA' AWS_SECRET_KEY = 'xyz') " +
      "FILE_FORMAT = (FORMAT_NAME = 'third_schema.format_name')"

    result must beEqualTo(expected)
  }

  def e3 = {
    val columns = List(
      Select.CastedColumn("orig_col", "dest_column", SnowflakeDatatype.Variant),
      Select.CastedColumn("orig_col", "next", SnowflakeDatatype.DoublePrecision),
      Select.CastedColumn("orig_col", "third", SnowflakeDatatype.Number(1, 2), Some(Substring(1, 255))))
    val select = Select(columns, "some_schema", "tmp_table")
    val input = Insert.InsertQuery("not_atomic", "events", List("one", "two", "three"), select)

    val result = input.getStatement.value
    val expected = "INSERT INTO not_atomic.events(one,two,three) " +
      "SELECT orig_col:dest_column::VARIANT, orig_col:next::DOUBLE PRECISION, substr(orig_col:third::NUMBER(1,2),1,255) " +
      "FROM some_schema.tmp_table"

    result must beEqualTo(expected)
  }

  def e4 = {
    val ast = Show.ShowStages(Some("s3://archive"), Some("atomic"))
    val result = ast.getStatement.value
    val expected = "SHOW stages LIKE 's3://archive' IN atomic"
    result must beEqualTo(expected)
  }

  def e5 = {
    val columns = List("id", "foo", "fp_id", "json")
    val input = CopyInto(
      "some_schema",
      "some_table",
      columns,
      CopyInto.From("other_schema", "stage_name", "path/to/dir"),
      None,
      CopyInto.FileFormat("third_schema", "format_name"),
      Some(CopyInto.SkipFileNum(10000)),
      false)

    val result = input.getStatement.value
    val expected = "COPY INTO some_schema.some_table(id,foo,fp_id,json) " +
      "FROM @other_schema.stage_name/path/to/dir " +
      "ON_ERROR = SKIP_FILE_10000 " +
      "FILE_FORMAT = (FORMAT_NAME = 'third_schema.format_name')"

    result must beEqualTo(expected)
  }

  def e6 = {
    val statement = CreateStage("snowplow_stage", Config.S3Folder.coerce("s3://cross-batch"), "JSON", "atomic", Some(Common.AwsCreds("ACCESS", "secret", None, None)))

    val result = statement.getStatement.value
    val expected = "CREATE STAGE IF NOT EXISTS atomic.snowplow_stage URL = 's3://cross-batch/' FILE_FORMAT = JSON CREDENTIALS = (AWS_KEY_ID = 'ACCESS' AWS_SECRET_KEY = 'secret')"

    result must beEqualTo(expected)
  }

  def e7 = {
    val columns = List("id", "foo", "fp_id", "json")
    val input = CopyInto(
      "some_schema",
      "some_table",
      columns,
      CopyInto.From("other_schema", "stage_name", "path/to/dir"),
      None,
      CopyInto.FileFormat("third_schema", "format_name"),
      Some(CopyInto.SkipFileNum(10000)),
      true)

    val result = input.getStatement.value
    val expected = "COPY INTO some_schema.some_table(id,foo,fp_id,json) " +
      "FROM @other_schema.stage_name/path/to/dir " +
      "ON_ERROR = SKIP_FILE_10000 " +
      "FILE_FORMAT = (FORMAT_NAME = 'third_schema.format_name' " +
      "STRIP_NULL_VALUES = TRUE)"

    result must beEqualTo(expected)
  }

  def e8 = {
    val input = CreateWarehouse(
      "snowplow_wh",
      Some(CreateWarehouse.Small),
      Some(500),
      Some(false))

    val result = input.getStatement.value
    val expected = "CREATE WAREHOUSE IF NOT EXISTS snowplow_wh " +
      "WAREHOUSE_SIZE = SMALL " +
      "AUTO_SUSPEND = 500 " +
      "AUTO_RESUME = FALSE"

    result must beEqualTo(expected)
  }

  def e9 = {
    val columns = List("id", "foo", "fp_id", "json")
    val input = CopyInto(
      "some_schema",
      "some_table",
      columns,
      CopyInto.From("other_schema", "stage_name", "path/to/dir"),
      Some(Common.AwsCreds("AAA", "xyz", None, Some("snplow_role"))),
      CopyInto.FileFormat("third_schema", "format_name"),
      None,
      stripNullValues = false)

    val result = input.getStatement.value
    val expected = "COPY INTO some_schema.some_table(id,foo,fp_id,json) " +
      "FROM @other_schema.stage_name/path/to/dir " +
      "CREDENTIALS = (AWS_ROLE = 'snplow_role') " +
      "FILE_FORMAT = (FORMAT_NAME = 'third_schema.format_name')"

    result must beEqualTo(expected)
  }

  def e10 = {
    val input = CreateStage(
      "sp_stage", Config.S3Folder("s3path"), "format", "schema", Some(Common.AwsCreds("", "", None, Some("sp_role")))
    )

    val result = input.getStatement.value
    val expected = "CREATE STAGE IF NOT EXISTS schema.sp_stage URL = 's3path' FILE_FORMAT = format " +
      "CREDENTIALS = (AWS_ROLE = 'sp_role')"

    result must beEqualTo(expected)
  }
}
