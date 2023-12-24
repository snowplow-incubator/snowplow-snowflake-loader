/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.IO
import fs2.{Chunk, Stream}
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import net.snowflake.ingest.utils.{ErrorCode, SFException}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.time.Instant
import scala.concurrent.duration.DurationLong

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment.Action
import com.snowplowanalytics.snowplow.sources.TokenedEvents

class ProcessingSpec extends Specification with CatsEffect {
  import ProcessingSpec._

  def is = s2"""
  The snowflake loader should:
    Insert events to Snowflake and ack the events $e1
    Emit BadRows when there are badly formatted events $e2
    Write good batches and bad events when input contains both $e3
    Alter the Snowflake table when the Channel reports missing columns $e4
    Emit BadRows when the Channel reports a problem with the data $e5
    Abort processing and don't ack events when the Channel reports a runtime error $e6
    Reset the Channel when the Channel reports the channel has become invalid $e7
    Set the latency metric based off the message timestamp $e8
  """

  def e1 =
    setup(generateEvents.take(2).compile.toList) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(4),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }

  def e2 =
    setup(generateBadlyFormatted.take(3).compile.toList) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(0),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }

  def e3 = {
    val toInputs = for {
      bads <- generateBadlyFormatted.take(3).compile.toList
      goods <- generateEvents.take(3).compile.toList
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack, None)
    }
    setup(toInputs) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(6),
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(6),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }
  }

  def e4 = {
    val mockedChannelResponses = List(
      Channel.WriteResult.WriteFailures(
        List(
          Channel.WriteFailure(0L, List("unstruct_event_xyz_1", "contexts_abc_2"), new SFException(ErrorCode.INVALID_FORMAT_ROW))
        )
      ),
      Channel.WriteResult.WriteFailures(Nil)
    )

    setup(generateEvents.take(1).compile.toList, mockedChannelResponses) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.ClosedChannel,
          Action.AlterTableAddedColumns(List("unstruct_event_xyz_1", "contexts_abc_2")),
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e5 = {
    val mockedChannelResponses = List(
      Channel.WriteResult.WriteFailures(
        List(
          Channel.WriteFailure(0L, Nil, new SFException(ErrorCode.INVALID_FORMAT_ROW))
        )
      ),
      Channel.WriteResult.WriteFailures(Nil)
    )

    setup(generateEvents.take(1).compile.toList, mockedChannelResponses) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.SentToBad(1),
          Action.AddedGoodCountMetric(1),
          Action.AddedBadCountMetric(1),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e6 = {
    val mockedChannelResponses = List(
      Channel.WriteResult.WriteFailures(
        List(
          Channel.WriteFailure(0L, Nil, new SFException(ErrorCode.INTERNAL_ERROR))
        )
      ),
      Channel.WriteResult.WriteFailures(Nil)
    )

    setup(generateEvents.take(1).compile.toList, mockedChannelResponses) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.handleError(_ => ())
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1)
        )
      )
    }
  }

  def e7 = {
    val mockedChannelResponses = List(
      Channel.WriteResult.ChannelIsInvalid,
      Channel.WriteResult.WriteFailures(Nil)
    )

    setup(generateEvents.take(1).compile.toList, mockedChannelResponses) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.ClosedChannel,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(2),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
  }

  def e8 = {
    val messageTime = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime = Instant.parse("2023-10-24T10:00:42.123Z")

    val toInputs = generateEvents.take(2).compile.toList.map {
      _.map {
        _.copy(earliestSourceTstamp = Some(messageTime))
      }
    }

    val io = setup(toInputs) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.SetLatencyMetric(42123),
          Action.SetLatencyMetric(42123),
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(4),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }

    TestControl.executeEmbed(io)

  }

}

object ProcessingSpec {

  def setup[A](
    toInputs: IO[List[TokenedEvents]],
    channelResponses: List[Channel.WriteResult] = Nil
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    toInputs.flatMap { inputs =>
      MockEnvironment.build(inputs, channelResponses).use { control =>
        f(inputs, control)
      }
    }

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
        val serialized = Chunk(event1, event2).map { e =>
          ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
        }
        TokenedEvents(serialized, ack, None)
      }
    }.repeat

  def generateBadlyFormatted: Stream[IO, TokenedEvents] =
    Stream.eval {
      IO.unique.map { token =>
        val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
        TokenedEvents(serialized, token, None)
      }
    }.repeat

}
