/*
 * Copyright (c) 2017-2021 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowflake.transformer

import java.time.Instant

import cats.implicits._

import io.circe.syntax._

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event

import com.snowplowanalytics.snowplow.badrows.{ BadRow, Payload }
import com.snowplowanalytics.snowplow.badrows.Failure.GenericFailure

/**
  * Place where errors specific to Snowflake are checked
  */
object SnowflakeErrorCheck {

  private val timestampZero = Instant.parse("0000-01-01T00:00:00.00Z").getEpochSecond

  def apply(event: Event): Option[BadRow] = {
    checkTimestampsForSnowflakeSpecificRange(event)
  }

  /**
    * Checks timestamp fields in the event for Snowflake specific range
    * @param event event
    * @return SnowflakeFailure if there are timestamps which can not pass the check,
    *         None otherwise
    */
  private def checkTimestampsForSnowflakeSpecificRange(event: Event): Option[BadRow] = {
    val errList = List(
      lessThanZero(Some(event.collector_tstamp), "collector_tstamp"),
      lessThanZero(event.derived_tstamp, "derived_tstamp"),
      lessThanZero(event.dvce_created_tstamp, "dvce_created_tstamp"),
      lessThanZero(event.dvce_sent_tstamp, "dvce_sent_tstamp"),
      lessThanZero(event.etl_tstamp, "etl_tstamp"),
      lessThanZero(event.refr_dvce_tstamp, "refr_dvce_tstamp"),
      lessThanZero(event.true_tstamp, "true_tstamp")
    )
    errList.flatten.toNel.map { failures =>
      BadRow.GenericError(Transformer.processor, GenericFailure(Instant.now(), failures), Payload.RawPayload(event.asJson.noSpaces))
    }
  }

  private def lessThanZero(timestamp: Option[Instant], column: String): Option[String] =
    for {
      ts <- timestamp
      if ts.getEpochSecond < timestampZero
    } yield s"Timestamp ${ts.toString} is out of Snowflake range, at column $column"
}
