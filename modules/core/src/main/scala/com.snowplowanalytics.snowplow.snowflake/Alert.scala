/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import cats.Show
import cats.implicits.showInterpolator

import com.snowplowanalytics.snowplow.runtime.SetupExceptionMessages

sealed trait Alert
object Alert {

  final case class FailedToConnectToSnowflake(cause: SetupExceptionMessages) extends Alert
  final case class FailedToShowTables(
    database: String,
    schema: String,
    cause: SetupExceptionMessages
  ) extends Alert
  final case class FailedToCreateEventsTable(
    database: String,
    schema: String,
    cause: SetupExceptionMessages
  ) extends Alert
  final case class FailedToAddColumns(columns: List[String], cause: SetupExceptionMessages) extends Alert
  final case class FailedToOpenSnowflakeChannel(cause: SetupExceptionMessages) extends Alert
  final case class FailedToParsePrivateKey(cause: SetupExceptionMessages) extends Alert
  final case class TableIsMissingAtomicColumn(columnName: String) extends Alert

  implicit def showAlert: Show[Alert] = Show {
    case FailedToConnectToSnowflake(cause)            => show"Failed to connect to Snowflake: $cause"
    case FailedToShowTables(db, schema, cause)        => show"Failed to SHOW tables in Snowflake schema $db.$schema: $cause"
    case FailedToCreateEventsTable(db, schema, cause) => show"Failed to create table in schema $db.$schema: $cause"
    case FailedToAddColumns(columns, cause)           => show"Failed to add columns: ${columns.mkString("[", ",", "]")}. Cause: $cause"
    case FailedToOpenSnowflakeChannel(cause)          => show"Failed to open Snowflake channel: $cause"
    case FailedToParsePrivateKey(cause)               => show"Failed to parse private key: $cause"
    case TableIsMissingAtomicColumn(colName) => show"Existing table is incompatible with Snowplow: Missing required column $colName"
  }

}
