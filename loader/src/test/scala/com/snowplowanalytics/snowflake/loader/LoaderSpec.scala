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
package com.snowplowanalytics.snowflake.loader

import org.specs2.matcher.Matcher
import org.specs2.mutable.Specification
import org.specs2.matcher.MatchersImplicits._

import org.joda.time.DateTime

import cats.effect.{Clock, ExitCode, IO, Sync}
import cats.effect.concurrent.Ref

import fs2.Stream

import com.snowplowanalytics.snowflake.core.{Config, ProcessManifest, RunId}
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}
import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.loader.connection.{Database, DryRun}

class LoaderSpec extends Specification {

  "getShreddedType" should {
    "parse context column name as ARRAY type" in {
      val columnName = "contexts_com_snowplowanalytics_snowplow_web_page_1"
      val expected = "contexts_com_snowplowanalytics_snowplow_web_page_1" -> SnowflakeDatatype.JsonArray
      val result = Loader.getShredType(columnName)

      result must beRight(expected)
    }

    "parse unstruct event column name as OBJECT type" in {
      val columnName = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"
      val expected = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" -> SnowflakeDatatype.JsonObject
      val result = Loader.getShredType(columnName)

      result must beRight(expected)
    }

    "fail to parse invalid column name" in {
      // starts with context_ instead of contexts_
      val columnName = "context_com_snowplowanalytics_snowplow_web_page_1"
      val result = Loader.getShredType(columnName)

      result must beLeft
    }
  }

  "getColumns" should {
    "parse list of valid column names" in {
      val newColumns = List("contexts_com_acme_ctx_1", "unstruct_event_something_1", "unstruct_event_another_2")
      Loader.getColumns(newColumns) must beRight.like {
        case columns => columns must LoaderSpec.endWith(List(
          ("contexts_com_acme_ctx_1",SnowflakeDatatype.JsonArray),
          ("unstruct_event_something_1",SnowflakeDatatype.JsonObject),
          ("unstruct_event_another_2",SnowflakeDatatype.JsonObject)
        ))
      }

    }

    "fail with Left if list contains invalid column name" in {
      val newColumns = List("contexts_com_acme_ctx_1", "unknown_type", "another")
      Loader.getColumns(newColumns) must beLeft("Columns [unknown_type, another] are not valid shredded types")
    }
  }

  "getInsertStatement" should {
    "build a valid INSERT statement" in {
      val config = Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "accessKey",
          secretAccessKey = "secretKey"
        ),
        awsRegion = "awsRegion",
        manifest = "snoflake-manifest",
        snowflakeRegion = "ue-east-1",
        stage = "snowplow-stage",
        stageUrl = Config.S3Folder.coerce("s3://somestage/foo"),
        badOutputUrl = Some(Config.S3Folder.coerce("s3://someBadRows/foo")),
        username = "snowfplow-loader",
        password = Config.PasswordConfig.PlainText("super-secret"),
        input = Config.S3Folder.coerce("s3://snowflake/input/"),
        account = "snowplow-account",
        warehouse = "snowplow_wa",
        database = "database",
        schema = "not_an_atomic",
        maxError = None,
        jdbcHost = None)

      val runId = RunId.ProcessedRunId(
        "archive/enriched/run=2017-10-09-17-40-30/",
        addedAt = DateTime.now(),       // Doesn't matter
        processedAt = DateTime.now(),   // Doesn't matter
        List(
          "contexts_com_snowplowanalytics_snowplow_web_page_1",
          "contexts_com_snowplowanalytics_snowplow_web_page_2",
          "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"),
        s3("s3://acme-snowplow/snowflake/run=2017-10-09-17-40-30/"),
        "some-script",
        false)

      val result = Loader.getInsertStatement(config, runId)

      result must beRight.like {
        case Insert.InsertQuery(schema, table, columns, Select(sColumns, sSchema, sTable)) =>
          // INSERT
          val schemaResult = schema must beEqualTo("not_an_atomic")
          val tableResult = table must beEqualTo("events")
          val columnsAmount = columns must haveLength(131)
          val exactColumns = columns must containAllOf(List(
            "unstruct_event_com_snowplowanalytics_snowplow_link_click_1",
            "contexts_com_snowplowanalytics_snowplow_web_page_2",
            "app_id",
            "page_url"))

          // SELECT
          val sSchemaResult = sSchema must beEqualTo("not_an_atomic")
          val sTableResult = sTable must beEqualTo("snowplow_tmp_run_2017_10_09_17_40_30")
          val sColumnsAmount = sColumns must haveLength(131)
          val sExactColumns = sColumns must containAllOf(List(
            Select.CastedColumn("enriched_data","unstruct_event_com_snowplowanalytics_snowplow_link_click_1", SnowflakeDatatype.JsonObject),
            Select.CastedColumn("enriched_data", "contexts_com_snowplowanalytics_snowplow_web_page_1", SnowflakeDatatype.JsonArray),
            Select.CastedColumn("enriched_data", "true_tstamp", SnowflakeDatatype.Timestamp),
            Select.CastedColumn("enriched_data", "refr_domain_userid", SnowflakeDatatype.Varchar(Some(128)))))

          schemaResult.and(tableResult).and(columnsAmount)
            .and(sSchemaResult).and(sTableResult).and(sColumnsAmount)
            .and(exactColumns).and(sExactColumns)
      }
    }
  }

  "run" should {
    "perform known list of SQL statements" in {
      val config = Config(
        auth = Config.AuthMethod.CredentialsAuth("access", "secret"),
        "us-east-1",
        "manifest",
        "eu-central-1",
        "archive-stage",
        Config.S3Folder.coerce("s3://archive/"),
        Some(Config.S3Folder.coerce("s3://enriched-input/")),
        Config.S3Folder.coerce("s3://someBadRows/foo"),
        "user",
        Config.PasswordConfig.PlainText("pass"),
        "snowplow-acc",
        "wh",
        "db",
        "atomic",
        None,
        None)

      val appName = "Snowplow_OSS"

      import LoaderSpec._

      val expectedSql = List(
        "SHOW schemas LIKE 'atomic'",
        "SHOW stages LIKE 'archive-stage' IN atomic",
        "SHOW tables LIKE 'events' IN atomic",
        "SHOW file formats LIKE 'snowplow_enriched_json' IN atomic",
        "SHOW warehouses LIKE 'wh'",
        "USE WAREHOUSE wh",
        "ALTER WAREHOUSE wh RESUME",
        "New transaction snowplow_run_2017_12_10_14_30_35 started",
        "ALTER TABLE atomic.events ADD COLUMN contexts_com_acme_something_1 ARRAY",
        "CREATE TEMPORARY TABLE IF NOT EXISTS atomic.snowplow_tmp_run_2017_12_10_14_30_35 (enriched_data OBJECT NOT NULL)",
        // INSERT INTO
        "Transaction [snowplow_run_2017_12_10_14_30_35] successfully closed"
      )

      val expectedLoaded = List("enriched/good/run=2017-12-10-14-30-35")

      val noopLogger = Logger.initNoop[IO]
      val test = for {
        connection <- Database[IO].getConnection(config, appName)
        manifestState <- Ref.of[IO, ManifestState](LoaderSpec.ManifestState(Nil))
        code <- Loader.run(connection, config)(Sync[IO], Database[IO], new ProcessingManifestTest(manifestState), noopLogger)
        messages <- connection match {
          case Database.Connection.Dry(state) => state.get.map(_.messages.reverse.filterNot(_.startsWith("INSERT INTO")))
          case _ => IO.raiseError(new RuntimeException("Unexpected connection type"))
        }
        loaded <- manifestState.get.map(_.loaded)
      } yield (code must beEqualTo(ExitCode.Success)) and (messages must containAllOf(expectedSql).inOrder) and (loaded must beEqualTo(expectedLoaded))

      test.unsafeRunSync()
    }
  }
}

object LoaderSpec {

  implicit val catsIoClock: Clock[IO] = Clock.create[IO]

  implicit val catsIoDryRunDatabase: Database[IO] = new DryRun.Stub {
    override def executeAndReturnResult[S: Statement](connection: Database.Connection, ast: S) =
      ast.getStatement match {
        case Statement.SqlStatement(str) if str.toLowerCase.contains("show stages") =>
          IO.pure(List(Map("url" -> "s3://archive/")))
        case _ =>
          super.executeAndReturnResult(connection, ast)
      }
  }

  case class ManifestState(loaded: List[String])

  class ProcessingManifestTest(state: Ref[IO, ManifestState]) extends ProcessManifest[IO] {
    def markLoaded(tableName: String, runid: String): IO[Unit] =
      state.update(s => s.copy(loaded = runid :: s.loaded))

    def scan(tableName: String): Stream[IO, RunId] =
      Stream.emit(
        RunId.ProcessedRunId(
          "enriched/good/run=2017-12-10-14-30-35",
          DateTime.parse("2017-12-10T01:20+02:00"),
          DateTime.parse("2017-12-10T01:20+02:00"),
          List("contexts_com_acme_something_1"),
          Config.S3Folder.coerce("s3://archive/run=2017-12-10-14-30-35/"), "0.2.0", false)
      ).covary[IO]

    def getUnprocessed(manifestTable: String, enrichedInput: Config.S3Folder): IO[List[String]] = ???
    def add(tableName: String, runId: String): IO[Unit] = ???
    def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): IO[Unit] = ???
  }

  def endWith[A](expectation: List[A]): Matcher[List[A]] = { s: List[A] =>
    (s.endsWith(expectation), s.toString +s" ends with $expectation", s.toString  + s" doesn't end with $expectation")
  }
}
