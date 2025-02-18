/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.IO
import cats.effect.testing.specs2.CatsEffect
import cats.effect.testkit.TestControl
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.snowflake.{MockEnvironment, RuntimeService}
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
    Mark app as unhealthy when sinking badrows fails $e8
    Mark app as unhealthy when writing to the Channel fails with runtime exception $e9
    Mark app as unhealthy when writing to the Channel fails SDK internal error exception $e10
  """

  def e1 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val io = runTest(inputEvents(count = 2, good(optCollectorTstamp = Option(collectorTstamp)))) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(4),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.AddedGoodCountMetric(4),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e2 =
    runTest(inputEvents(count = 3, badlyFormatted)) { case (inputs, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(0),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }

  def e3 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val toInputs = for {
      bads <- inputEvents(count = 3, badlyFormatted)
      goods <- inputEvents(count = 3, good(optCollectorTstamp = Option(collectorTstamp)))
    } yield bads.zip(goods).map { case (bad, good) =>
      TokenedEvents(bad.events ++ good.events, good.ack)
    }
    val io = runTest(toInputs) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(6),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.SentToBad(6),
          Action.AddedGoodCountMetric(6),
          Action.AddedBadCountMetric(6),
          Action.Checkpointed(List(inputs(0).ack, inputs(1).ack, inputs(2).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e4 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, List("unstruct_event_xyz_1", "contexts_abc_2"), newSFException(ErrorCode.INVALID_FORMAT_ROW))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
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
          Action.SetE2ELatencyMetric(42123.millis),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e5 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, Nil, newSFException(ErrorCode.INVALID_FORMAT_ROW))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.SentToBad(1),
          Action.AddedGoodCountMetric(1),
          Action.AddedBadCountMetric(1),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e6 = {
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, Nil, newSFException(ErrorCode.INTERNAL_ERROR))
            )
          )
        ),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )

    runTest(inputEvents(count = 1, good()), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.handleError(_ => ())
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.BecameUnhealthy(RuntimeService.Snowflake)
        )
      )
    }
  }

  def e7 = {
    val collectorTstamp = Instant.parse("2023-10-24T10:00:00.000Z")
    val processTime     = Instant.parse("2023-10-24T10:00:42.123Z")
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(Channel.WriteResult.ChannelIsInvalid),
        Response.Success(Channel.WriteResult.WriteFailures(Nil))
      )
    )
    val io = runTest(inputEvents(count = 1, good(optCollectorTstamp = Option(collectorTstamp))), mocks) { case (inputs, control) =>
      for {
        _ <- IO.sleep(processTime.toEpochMilli.millis)
        _ <- Processing.stream(control.environment).compile.drain
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.ClosedChannel,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(2),
          Action.SetE2ELatencyMetric(42123.millis),
          Action.AddedGoodCountMetric(2),
          Action.AddedBadCountMetric(0),
          Action.Checkpointed(List(inputs(0).ack))
        )
      )
    }
    TestControl.executeEmbed(io)
  }

  def e8 = {
    val mocks = Mocks.default.copy(
      badSinkResponse = Response.ExceptionThrown(new RuntimeException("Some error when sinking bad data"))
    )

    runTest(inputEvents(count = 1, badlyFormatted), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.BecameUnhealthy(RuntimeService.BadSink)
        )
      )
    }
  }

  def e9 = {
    val mocks = Mocks.default.copy(
      channelResponses = List(Response.ExceptionThrown(new RuntimeException("Some error when writing to the Channel")))
    )

    runTest(inputEvents(count = 1, good()), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.BecameUnhealthy(RuntimeService.Snowflake)
        )
      )
    }
  }

  def e10 = {
    val mocks = Mocks.default.copy(
      channelResponses = List(
        Response.Success(
          Channel.WriteResult.WriteFailures(
            List(
              Channel.WriteFailure(0L, List.empty, newSFException(ErrorCode.INTERNAL_ERROR))
            )
          )
        )
      )
    )

    runTest(inputEvents(count = 1, good()), mocks) { case (_, control) =>
      for {
        _ <- Processing.stream(control.environment).compile.drain.voidError
        state <- control.state.get
      } yield state should beEqualTo(
        Vector(
          Action.InitEventsTable,
          Action.OpenedChannel,
          Action.WroteRowsToSnowflake(1),
          Action.BecameUnhealthy(RuntimeService.Snowflake)
        )
      )
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

  def good(optCollectorTstamp: Option[Instant] = None): IO[TokenedEvents] =
    for {
      ack <- IO.unique
      eventId1 <- IO.randomUUID
      eventId2 <- IO.randomUUID
      now <- IO.realTimeInstant
      collectorTstamp = optCollectorTstamp.fold(now)(identity)
    } yield {
      val event1 = Event.minimal(eventId1, collectorTstamp, "0.0.0", "0.0.0")
      val event2 = Event.minimal(eventId2, collectorTstamp, "0.0.0", "0.0.0")
      val serialized = Chunk(event1, event2).map { e =>
        ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
      }
      TokenedEvents(serialized, ack)
    }

  def badlyFormatted: IO[TokenedEvents] =
    IO.unique.map { token =>
      val serialized = Chunk("nonsense1", "nonsense2").map(s => ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)))
      TokenedEvents(serialized, token)
    }

  // Helper to create a SFException, and remove the stacktrace so we don't clutter our test logs.
  private def newSFException(errorCode: ErrorCode): SFException = {
    val t = new SFException(errorCode)
    t.setStackTrace(Array()) // don't clutter our test logs
    t
  }

}
