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

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment
import com.snowplowanalytics.snowplow.snowflake.MockEnvironment.{Action, Mocks, Response}
import com.snowplowanalytics.snowplow.sources.TokenedEvents
import fs2.{Chunk, Stream}
import net.snowflake.ingest.utils.{ErrorCode, SFException}
import org.specs2.Specification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.concurrent.duration.DurationLong

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
    Mark app as unhealthy when sinking badrows fails $e9
    Mark app as unhealthy when writing to the Channel fails with runtime exception $e10
    Mark app as unhealthy when writing to the Channel fails SDK internal error exception $e11
  """

  def e1 =
    runTest(inputEvents(count = 2, good)) { case (inputs, control) =>
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
    runTest(inputEvents(count = 3, badlyFormatted)) { case (inputs, control) =>
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
      bads <- inputEvents(count = 3, badlyFormatted)
      goods <- inputEvents(count = 3, good)
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack, None)
    }
    runTest(toInputs) { case (inputs, control) =>
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
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, List("unstruct_event_xyz_1", "contexts_abc_2"), new SFException(ErrorCode.INVALID_FORMAT_ROW))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )

    runTest(inputEvents(count = 1, good), mocks) { case (inputs, control) =>
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
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, Nil, new SFException(ErrorCode.INVALID_FORMAT_ROW))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )

    runTest(inputEvents(count = 1, good), mocks) { case (inputs, control) =>
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
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, Nil, new SFException(ErrorCode.INTERNAL_ERROR))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )

    runTest(inputEvents(count = 1, good), mocks) { case (_, control) =>
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
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(Channel.WriteResult.ChannelIsInvalid),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )

    runTest(inputEvents(count = 1, good), mocks) { case (inputs, control) =>
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

    val toInputs = inputEvents(count = 2, good).map {
      _.map {
        _.copy(earliestSourceTstamp = Some(messageTime))
      }
    }

    val io = runTest(toInputs) { case (inputs, control) =>
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

  def e9 = {
    val mocks = Mocks.default.copy(
      badSinkResponse = Response.ExceptionThrown(new RuntimeException("Some error when sinking bad data"))
    )

    runTest(inputEvents(count = 1, badlyFormatted), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        healthStatus <- control.environment.appHealth.status()
      } yield healthStatus should beEqualTo(HealthProbe.Unhealthy("Bad sink is not healthy"))
    }
  }

  def e10 = {
    val mocks = Mocks.default.copy(
      channelResponses = List(Response.ExceptionThrown(new RuntimeException("Some error when writing to the Channel")))
    )

    runTest(inputEvents(count = 1, good), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        healthStatus <- control.environment.appHealth.status()
      } yield healthStatus should beEqualTo(HealthProbe.Unhealthy("Snowflake is not healthy"))
    }
  }

  def e11 = {
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, List.empty, new SFException(ErrorCode.INTERNAL_ERROR))
            )
          )
        )
      )
    )

    runTest(inputEvents(count = 1, good), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        healthStatus <- control.environment.appHealth.status()
      } yield healthStatus should beEqualTo(HealthProbe.Unhealthy("Snowflake is not healthy"))
    }
  }

}

object ProcessingSpec {

  def runTest[A](
    toInputs: IO[List[TokenedEvents]],
    mocks: Mocks = Mocks.default
  )(
    f: (List[TokenedEvents], MockEnvironment) => IO[A]
  ): IO[A] =
    toInputs.flatMap { inputs =>
      MockEnvironment.build(inputs, mocks).use { control =>
        f(inputs, control)
      }
    }

  def inputEvents(count: Long, source: IO[TokenedEvents]): IO[List[TokenedEvents]] =
    Stream
      .eval(source)
      .repeat
      .take(count)
      .compile
      .toList

  def good: IO[TokenedEvents] =
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

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token, None)
    }

}
