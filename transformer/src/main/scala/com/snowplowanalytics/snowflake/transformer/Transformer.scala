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

import java.util.UUID

import scala.util.control.NonFatal

import io.circe.Json
import io.circe.syntax._

import cats.syntax.either._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

import com.snowplowanalytics.snowflake.generated.ProjectMetadata

import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifest
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Payload}

object Transformer {

  val processor = Processor("snowplow-snowflake-transformer", ProjectMetadata.version)

  /**
    * Transform jsonified TSV to pair of shredded keys and enriched event in JSON format
    *
    * @param event Event case class instance
    * @return pair of set with column names and JValue
    */
  def transform(event: Event, atomicSchema: Schema): (Set[String], String) = {
    val shredTypes = event.inventory.map(item => SnowplowEvent.transformSchema(item.shredProperty, item.schemaKey))
    val atomic = atomicSchema.properties.map { properties => properties.value.mapValues { property =>
      property.maxLength.map {_.value.intValue}
    }}.getOrElse(throw new RuntimeException(s"Could not convert atomic schema to property map"))
    (shredTypes, truncateFields(event.toJson(true), atomic).noSpaces)
  }

  /**
    * Try to store event components in duplicate storage and check if it was stored before
    * If event is unique in storage - true will be returned,
    * If event is already in storage, with different etlTstamp - false will be returned,
    * If event is already in storage, but with same etlTstamp - true will be returned (previous shredding was interrupted),
    * If storage is not configured - true will be returned.
    * If provisioned throughput exception happened - interrupt whole job
    * If other runtime exception happened - failure is returned to be used as bad row
    * @param event whole enriched event with possibly faked fingerprint
    * @param duplicateStorage object dealing with possible duplicates
    * @return boolean inside validation, denoting presence or absence of event in storage
    */
  def dedupeCrossBatch(event: Event, duplicateStorage: Option[EventsManifest]): Either[BadRow, Boolean] = {
    duplicateStorage match {
      case Some(storage) =>
        try {
          val eventId = event.event_id
          val eventFingerprint = event.event_fingerprint.getOrElse(UUID.randomUUID().toString)
          val etlTstamp = event.etl_tstamp.getOrElse(throw new RuntimeException(s"etl_tstamp in event $eventId is empty or missing"))
          Right(storage.put(eventId, eventFingerprint, etlTstamp))
        } catch {
          case NonFatal(e) =>
            Left(BadRow.LoaderRuntimeError(processor, Option(e.getMessage).getOrElse(e.toString), Payload.LoaderPayload(event)))
        }
      case None => Right(true)
    }
  }

  /**
    * Transform TSV to pair of inventory items and JSON object
    *
    * @param line enriched event TSV
    * @return Event case class instance
    */
  def jsonify(line: String): Either[BadRow, Event] =
    Event.parse(line)
      .toEither
      .leftMap(error => BadRow.LoaderParsingError(processor, error, Payload.RawPayload(line)))

  /**
    * Truncate a Snowplow event's fields based on atomic schema
    */
  def truncateFields(eventJson: Json, atomic: Map[String, Option[Int]]): Json = {
    Json.fromFields(eventJson.asObject.getOrElse(throw new RuntimeException(s"Event JSON is not an object? $eventJson")).toList.map {
      case (key, value) if value.isString =>
        atomic.get(key) match {
          case Some(Some(length)) => (key, value.asString.map { s =>
            (if (s.length > length) s.take(length) else s).asJson
          }.getOrElse(value))
          case _ => (key, value)
        }
      case other => other
    })
  }

}
