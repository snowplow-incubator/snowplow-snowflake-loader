package com.snowplowanalytics.snowplow.snowflake

import com.snowplowanalytics.snowplow.runtime.AppInfo
import io.circe.{Json, parser}
import org.specs2.Specification
import org.specs2.matcher.MatchResult

import java.sql.SQLException

class AlertSpec extends Specification {

  private val testAppInfo: AppInfo = new AppInfo {
    override val name: String        = "testApp"
    override val version: String     = "testVersion"
    override val dockerAlias: String = "test-docker-alias"
    override val cloud: String       = "test-cloud"
  }

  private val configuredTags = Map(
    "testTag1" -> "testValue1",
    "testTag2" -> "testValue2"
  )

  def is = s2"""
    Serializing alerts should produce valid self-describing JSON for:
     FailedToCreateEventsTable alert $e1
     FailedToAddColumns alert $e2
     FailedToOpenSnowflakeChannel alert $e3
     Alert with SQL exception as a cause $e4
     Alert with nested exceptions as a cause $e5
     Alert with long message which should be trimmed to hardcoded max length $e6
  """

  def expectedFullAlertBody(message: String): String =
    s"""
    |{
    |  "schema" : "iglu:com.snowplowanalytics.monitoring.loader/alert/jsonschema/1-0-0",
    |  "data" : {
    |    "application" : "testApp-testVersion",
    |    "message" : "$message",
    |    "tags" : {
    |      "testTag1" : "testValue1",
    |      "testTag2" : "testValue2"
    |    }
    |  }
    |}
    """.stripMargin

  def e1: MatchResult[Json] = {
    val cause = new RuntimeException("Some details from exception")

    assert(
      inputAlert           = Alert.FailedToCreateEventsTable(cause),
      expectedAlertMessage = "Failed to create events table: Some details from exception"
    )
  }

  def e2: MatchResult[Json] = {
    val cause   = new RuntimeException("Some details from exception")
    val columns = List("unstruct_event_com_example_schema_1", "context_com_example_schema_2")

    assert(
      inputAlert = Alert.FailedToAddColumns(columns, cause),
      expectedAlertMessage =
        "Failed to add columns: [unstruct_event_com_example_schema_1,context_com_example_schema_2]. Cause: Some details from exception"
    )
  }

  def e3: MatchResult[Json] = {
    val cause = new RuntimeException("Some details from exception")

    assert(
      inputAlert           = Alert.FailedToOpenSnowflakeChannel(cause),
      expectedAlertMessage = "Failed to open Snowflake channel: Some details from exception"
    )
  }

  def e4: MatchResult[Json] = {
    val cause = new SQLException("Schema 'DB.TEST-SCHEMA' doesn't exist or not authorized", "SOME_SQL_STATE", 2003)
    assert(
      inputAlert = Alert.FailedToCreateEventsTable(cause),
      expectedAlertMessage =
        """Failed to create events table: Schema 'DB.TEST-SCHEMA' doesn't exist or not authorized = SqlState: SOME_SQL_STATE"""
    )
  }

  def e5: MatchResult[Json] = {
    val cause1 = new RuntimeException("Details from cause 1")
    val cause2 = new RuntimeException("Details from cause 2", cause1)
    val cause3 = new RuntimeException("Details from cause 3", cause2)

    assert(
      inputAlert           = Alert.FailedToCreateEventsTable(cause3),
      expectedAlertMessage = "Failed to create events table: Details from cause 3: Details from cause 2: Details from cause 1"
    )
  }

  def e6: MatchResult[Json] = {
    val cause = new RuntimeException("A" * 5000)

    assert(
      inputAlert           = Alert.FailedToCreateEventsTable(cause),
      expectedAlertMessage =
        // Limit is 4096, so 31 chars from 'Failed to create events table: ' + 4065 from 'A's
        s"Failed to create events table: ${"A" * 4065}"
    )
  }

  private def assert(inputAlert: Alert, expectedAlertMessage: String): MatchResult[Json] = {
    val inputJson = Alert.toSelfDescribingJson(
      inputAlert,
      testAppInfo,
      configuredTags
    )
    val outputJson = parser
      .parse(expectedFullAlertBody(expectedAlertMessage))
      .getOrElse(throw new IllegalArgumentException("Invalid JSON"))

    inputJson must beEqualTo(outputJson)
  }
}
