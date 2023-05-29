/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.IO
import fs2.Stream
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import java.nio.charset.StandardCharsets

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The snowflake loader should:
    Insert events to Snowflake and ack the events $e1
    Send badly formatted events to the bad sink $e2
    Write good batches and bad events when input contains both $e3
  """

  def e1 =
    for {
      inputs <- generateEvents.take(2).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.InsertedRows(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(0).ack)),
        Action.InsertedRows(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(0),
        Action.Checkpointed(List(inputs(1).ack))
      )
    )

  def e2 =
    for {
      inputs <- generateBadlyFormatted.take(3).compile.toList
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(0).ack)),
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(1).ack)),
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(0),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(2).ack))
      )
    )

  def e3 =
    for {
      bads <- generateBadlyFormatted.take(3).compile.toList
      goods <- generateEvents.take(3).compile.toList
      inputs = bads.zip(goods).map { case (bad, good) =>
                 TokenedEvents(bad.events ::: good.events, good.ack)
               }
      control <- MockEnvironment.build(inputs)
      _ <- Processing.stream(control.environment).compile.drain
      state <- control.state.get
    } yield state should beEqualTo(
      Vector(
        Action.InsertedRows(2),
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(0).ack)),
        Action.InsertedRows(2),
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(1).ack)),
        Action.InsertedRows(2),
        Action.SentToBad(2),
        Action.AddedGoodCountMetric(2),
        Action.AddedBadCountMetric(2),
        Action.Checkpointed(List(inputs(2).ack))
      )
    )
}

object ProcessingSpec {

  def generateEvents: Stream[IO, TokenedEvents] =
    Stream.eval {
      for {
        ack <- IO.unique
        eventId1 <- IO.randomUUID
        eventId2 <- IO.randomUUID
        collectorTstamp <- IO.realTimeInstant
      } yield {
        val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
        val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
        val serialized = List(event1, event2).map { e =>
          e.toTsv.getBytes(StandardCharsets.UTF_8)
        }
        TokenedEvents(serialized, ack)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = List("nonsense1", "nonsense2").map(_.getBytes(StandardCharsets.UTF_8))
        TokenedEvents(serialized, token)
      }
    }.repeat

}
