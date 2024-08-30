/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.Show
import cats.effect.Sync
import cats.implicits._
import retry._
import net.snowflake.ingest.connection.IngestResponseException

import java.lang.SecurityException
import scala.util.matching.Regex

import com.snowplowanalytics.snowplow.runtime.{AppHealth, Retrying, SetupExceptionMessages}
import com.snowplowanalytics.snowplow.snowflake.{Alert, Config, RuntimeService}

object SnowflakeRetrying {

  def withRetries[F[_]: Sync: Sleep, A](
    appHealth: AppHealth.Interface[F, Alert, RuntimeService],
    config: Config.Retries,
    toAlert: SetupExceptionMessages => Alert
  )(
    action: F[A]
  ): F[A] =
    Retrying.withRetries(appHealth, config.transientErrors, config.setupErrors, RuntimeService.Snowflake, toAlert, isSetupError)(action)

  /** Is an error associated with setting up Snowflake as a destination */
  private def isSetupError: PartialFunction[Throwable, String] = {
    case ire: IngestResponseException if ire.getErrorCode >= 400 && ire.getErrorCode < 500 =>
      val shown = ire.show
      if (shown.matches(""".*\bERR_TABLE_TYPE_NOT_SUPPORTED\b.*"""))
        "Table must not be in a transient database or transient schema: Snowflake streaming ingest SDK only supports permanent tables"
      else
        shown
    case ire: IngestResponseException if ire.getErrorCode === 513 =>
      // Snowflake returns a 513 HTTP status code if you try to connect to a host that does not exist
      "Unrecognized Snowflake account name or host name"
    case _: SecurityException =>
      "Unauthorized: Invalid user name or public/private key pair"
    case sql: java.sql.SQLException if Set(2003, 2043).contains(sql.getErrorCode) =>
      // Various known error codes for object does not exist or not authorized to view it
      sql.show
    case sql: java.sql.SQLException if sql.getErrorCode === 3001 =>
      // Insufficient privileges
      sql.show
  }

  private implicit def showSqlException: Show[java.sql.SQLException] = Show { sql =>
    sql.getMessage.replaceAll(":? *\n", ": ").replaceFirst("^(?i)sql compilation error: *", "")
  }

  private val ingestResponseBodyRegex: Regex = """"message" *: *"([^"]*)"""".r.unanchored

  private implicit def showIngestResponseException: Show[IngestResponseException] = Show { ire =>
    val maybeExtractedMsg = Option(ire.getErrorBody).flatMap(body => Option(body.getMessage))
    (maybeExtractedMsg, ire.getMessage) match {
      case (Some(message), _) =>
        // The SDK already extracted the friendly error message for us
        message
      case (None, ingestResponseBodyRegex(message)) =>
        // We extracted a friendly error message by pattern matching
        message
      case (None, other) =>
        // No friendly error message, but this is probably a user-caused error so return the full message anyway
        other
    }
  }

}
