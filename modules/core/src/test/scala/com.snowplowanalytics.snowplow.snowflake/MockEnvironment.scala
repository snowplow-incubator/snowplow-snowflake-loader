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

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import com.snowplowanalytics.snowplow.runtime.AppInfo
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.AppHealth.Service.{BadSink, Snowflake}
import com.snowplowanalytics.snowplow.snowflake.processing.{Channel, TableManager}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import fs2.Stream
import org.http4s.client.Client

import scala.concurrent.duration.{DurationInt, FiniteDuration}

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  private val everythingHealthy: Map[AppHealth.Service, Boolean] = Map(Snowflake -> true, BadSink -> true)

  sealed trait Action
  object Action {
    case object InitEventsTable extends Action
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case class AlterTableAddedColumns(columns: List[String]) extends Action
    case object ClosedChannel extends Action
    case object OpenedChannel extends Action
    case class WroteRowsToSnowflake(rowCount: Int) extends Action
    case class AddedGoodCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
    case class SetLatencyMetric(millis: Long) extends Action
  }
  import Action._

  def build(inputs: List[TokenedEvents], mocks: Mocks): Resource[IO, MockEnvironment] =
    for {
      state <- Resource.eval(Ref[IO].of(Vector.empty[Action]))
      source = testSourceAndAck(inputs, state)
      channelResource <- Resource.eval(testChannel(mocks.channelResponses, state))
      channelColdswap <- Coldswap.make(channelResource)
      appHealth <- Resource.eval(AppHealth.init(10.seconds, source, everythingHealthy))
    } yield {
      val env = Environment(
        appInfo      = appInfo,
        source       = source,
        badSink      = testBadSink(mocks.badSinkResponse, state),
        httpClient   = testHttpClient,
        tableManager = testTableManager(state),
        channel      = channelColdswap,
        metrics      = testMetrics(state),
        appHealth    = appHealth,
        batching = Config.Batching(
          maxBytes          = 16000000,
          maxDelay          = 10.seconds,
          uploadConcurrency = 1
        ),
        schemasToSkip = List.empty,
        badRowMaxSize = 1000000
      )
      MockEnvironment(state, env)
    }

  final case class Mocks(
    channelResponses: List[Response[Channel.WriteResult]],
    badSinkResponse: Response[Unit]
  )

  object Mocks {
    val default: Mocks = Mocks(channelResponses = List.empty, badSinkResponse = Response.Success(()))
  }

  sealed trait Response[+A]
  object Response {
    final case class Success[A](value: A) extends Response[A]
    final case class ExceptionThrown(value: Throwable) extends Response[Nothing]
  }

  val appInfo = new AppInfo {
    def name        = "snowflake-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/snowflake-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testTableManager(state: Ref[IO, Vector[Action]]): TableManager[IO] = new TableManager[IO] {

    override def initializeEventsTable(): IO[Unit] =
      state.update(_ :+ InitEventsTable)

    def addColumns(columns: List[String]): IO[Unit] =
      state.update(_ :+ AlterTableAddedColumns(columns))
  }

  private def testSourceAndAck(inputs: List[TokenedEvents], state: Ref[IO, Vector[Action]]): SourceAndAck[IO] =
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig, processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .emits(inputs)
          .through(processor)
          .chunks
          .evalMap { chunk =>
            state.update(_ :+ Checkpointed(chunk.toList))
          }
          .drain

      override def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)
    }

  private def testBadSink(mockedResponse: Response[Unit], state: Ref[IO, Vector[Action]]): Sink[IO] =
    Sink[IO] { batch =>
      mockedResponse match {
        case Response.Success(_) =>
          state.update(_ :+ SentToBad(batch.asIterable.size))
        case Response.ExceptionThrown(value) =>
          IO.raiseError(value)
      }
    }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  private def testChannel(
    mockedResponses: List[Response[Channel.WriteResult]],
    actionRef: Ref[IO, Vector[Action]]
  ): IO[Resource[IO, Channel[IO]]] =
    Ref[IO].of(mockedResponses).map { responses =>
      val make = actionRef.update(_ :+ OpenedChannel).as {
        new Channel[IO] {
          def write(rows: Iterable[Map[String, AnyRef]]): IO[Channel.WriteResult] =
            for {
              response <- responses.modify {
                            case head :: tail => (tail, head)
                            case Nil          => (Nil, Response.Success(Channel.WriteResult.WriteFailures(Nil)))
                          }
              writeResult <- response match {
                               case success: Response.Success[Channel.WriteResult] =>
                                 updateActions(actionRef, rows, success) *> IO(success.value)
                               case Response.ExceptionThrown(ex) =>
                                 IO.raiseError(ex)
                             }
            } yield writeResult

          def updateActions(
            state: Ref[IO, Vector[Action]],
            rows: Iterable[Map[String, AnyRef]],
            success: Response.Success[Channel.WriteResult]
          ): IO[Unit] =
            success.value match {
              case Channel.WriteResult.WriteFailures(failures) =>
                state.update(_ :+ WroteRowsToSnowflake(rows.size - failures.size))
              case Channel.WriteResult.ChannelIsInvalid =>
                IO.unit
            }

        }
      }
      Resource.make(make)(_ => actionRef.update(_ :+ ClosedChannel))
    }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Int): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Int): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def setLatencyMillis(latencyMillis: Long): IO[Unit] =
      ref.update(_ :+ SetLatencyMetric(latencyMillis))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
