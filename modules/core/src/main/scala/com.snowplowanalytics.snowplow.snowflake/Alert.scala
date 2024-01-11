/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake

import cats.Show
import cats.implicits.showInterpolator
import com.snowplowanalytics.iglu.core.circe.implicits.igluNormalizeDataJson
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}
import com.snowplowanalytics.snowplow.runtime.AppInfo
import io.circe.Json
import io.circe.syntax.EncoderOps

import java.sql.SQLException

sealed trait Alert
object Alert {

  /** Restrict the length of an alert message to be compliant with alert iglu schema */
  private val MaxAlertPayloadLength = 4096

  final case class FailedToCreateEventsTable(cause: Throwable) extends Alert
  final case class FailedToAddColumns(columns: List[String], cause: Throwable) extends Alert
  final case class FailedToOpenSnowflakeChannel(cause: Throwable) extends Alert
  final case class FailedToParsePrivateKey(cause: Throwable) extends Alert

  def toSelfDescribingJson(
    alert: Alert,
    appInfo: AppInfo,
    tags: Map[String, String]
  ): Json =
    SelfDescribingData(
      schema = SchemaKey("com.snowplowanalytics.monitoring.loader", "alert", "jsonschema", SchemaVer.Full(1, 0, 0)),
      data = Json.obj(
        "appName" -> appInfo.name.asJson,
        "appVersion" -> appInfo.version.asJson,
        "message" -> getMessage(alert).asJson,
        "tags" -> tags.asJson
      )
    ).normalize

  private def getMessage(alert: Alert): String = {
    val full = alert match {
      case FailedToCreateEventsTable(cause)    => show"Failed to create events table: $cause"
      case FailedToAddColumns(columns, cause)  => show"Failed to add columns: ${columns.mkString("[", ",", "]")}. Cause: $cause"
      case FailedToOpenSnowflakeChannel(cause) => show"Failed to open Snowflake channel: $cause"
      case FailedToParsePrivateKey(cause)      => show"Failed to parse private key: $cause"
    }

    full.take(MaxAlertPayloadLength)
  }

  private implicit def throwableShow: Show[Throwable] = {
    def go(acc: List[String], next: Throwable): String = {
      val nextMessage = next match {
        case t: SQLException => Some(s"${t.getMessage} = SqlState: ${t.getSQLState}")
        case t               => Option(t.getMessage)
      }
      val msgs = nextMessage.filterNot(msg => acc.headOption.contains(msg)) ++: acc

      Option(next.getCause) match {
        case Some(cause) => go(msgs, cause)
        case None        => msgs.reverse.mkString(": ")
      }
    }

    Show.show(go(Nil, _))
  }
}
