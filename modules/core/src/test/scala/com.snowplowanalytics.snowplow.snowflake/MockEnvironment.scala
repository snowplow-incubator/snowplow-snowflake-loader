/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.effect.IO
import cats.effect.kernel.{Ref, Resource, Unique}
import org.http4s.client.Client
import fs2.Stream

import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.{ChannelProvider, TableManager}
import com.snowplowanalytics.snowplow.loaders.AppInfo

import scala.concurrent.duration.DurationInt

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case class AlterTableAddedColumns(columns: List[String]) extends Action
    case object ClosedChannel extends Action
    case object OpenedChannel extends Action
    case object FlushedChannel extends Action
    case class EnqueuedRows(rowCount: Int) extends Action
    case class AddedGoodCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @param channelResponses
   *   Responses we want the `ChannelProvider` to return when someone calls `enqueue`
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents], channelResponses: List[List[ChannelProvider.EnqueueFailure]] = Nil): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
      channelProvider <- testChannelProvider(state, channelResponses)
    } yield {
      val env = Environment(
        appInfo         = appInfo,
        source          = testSourceAndAck(inputs, state),
        badSink         = testSink(state),
        httpClient      = testHttpClient,
        tblManager      = testTableManager(state),
        channelProvider = channelProvider,
        metrics         = testMetrics(state),
        batching = Config.Batching(
          maxBytes          = 16000000,
          maxDelay          = 10.seconds,
          uploadConcurrency = 1
        )
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name        = "snowflake-loader-test"
    def version     = "0.0.0"
    def dockerAlias = "snowplow/snowflake-loader-test:0.0.0"
    def cloud       = "OnPrem"
  }

  private def testTableManager(state: Ref[IO, Vector[Action]]): TableManager[IO] = new TableManager[IO] {
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
    }

  private def testSink(ref: Ref[IO, Vector[Action]]): Sink[IO] = Sink[IO] { batch =>
    ref.update(_ :+ SentToBad(batch.size))
  }

  private def testHttpClient: Client[IO] = Client[IO] { _ =>
    Resource.raiseError[IO, Nothing, Throwable](new RuntimeException("http failure"))
  }

  /**
   * Mocked implementation of a `ChannelProvider`
   *
   * @param actionRef
   *   Global Ref used to accumulate actions that happened
   * @param responses
   *   Responses that this mocked ChannelProvider should return each time someone calls `enqueue`.
   *   If no responses given, then it will return with a successful response.
   */
  private def testChannelProvider(
    actionRef: Ref[IO, Vector[Action]],
    responses: List[List[ChannelProvider.EnqueueFailure]]
  ): IO[ChannelProvider[IO]] =
    for {
      responseRef <- Ref[IO].of(responses)
    } yield new ChannelProvider[IO] {
      def withClosedChannel[A](fa: IO[A]): IO[A] =
        for {
          _ <- actionRef.update(_ :+ ClosedChannel)
          a <- fa
          _ <- actionRef.update(_ :+ OpenedChannel)
        } yield a

      def enqueue(rows: Seq[Map[String, AnyRef]]): IO[List[ChannelProvider.EnqueueFailure]] =
        for {
          response <- responseRef.modify {
                        case head :: tail => (tail, head)
                        case Nil          => (Nil, Nil)
                      }
          _ <- actionRef.update(_ :+ EnqueuedRows(rows.size - response.size))
        } yield response

      def flush: IO[Unit] =
        actionRef.update(_ :+ FlushedChannel)
    }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Int): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Int): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
