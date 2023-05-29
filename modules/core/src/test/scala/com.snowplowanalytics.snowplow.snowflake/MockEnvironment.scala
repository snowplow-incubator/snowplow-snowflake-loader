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

case class MockEnvironment(state: Ref[IO, Vector[MockEnvironment.Action]], environment: Environment[IO])

object MockEnvironment {

  sealed trait Action
  object Action {
    case class Checkpointed(tokens: List[Unique.Token]) extends Action
    case class SentToBad(count: Int) extends Action
    case class AddedColumns(columns: List[String]) extends Action
    case object ResetChannel extends Action
    case class InsertedRows(rowCount: Int) extends Action
    case class AddedGoodCountMetric(count: Int) extends Action
    case class AddedBadCountMetric(count: Int) extends Action
  }
  import Action._

  /**
   * Build a mock environment for testing
   *
   * @param inputs
   *   Input events to send into the environment.
   * @return
   *   An environment and a Ref that records the actions make by the environment
   */
  def build(inputs: List[TokenedEvents]): IO[MockEnvironment] =
    for {
      state <- Ref[IO].of(Vector.empty[Action])
    } yield {
      val env = Environment(
        appInfo = appInfo,
        source = testSourceAndAck(inputs, state),
        badSink = testSink(state),
        httpClient = testHttpClient,
        tblManager = testTableManager(state),
        channelProvider = testChannelProvider(state),
        metrics = testMetrics(state)
      )
      MockEnvironment(state, env)
    }

  val appInfo = new AppInfo {
    def name = "snowflake-loader-test"
    def version = "0.0.0"
    def dockerAlias = "snowplow/snowflake-loader-test:0.0.0"
    def cloud = "OnPrem"
  }

  private def testTableManager(state: Ref[IO, Vector[Action]]): TableManager[IO] = new TableManager[IO] {
    def addColumns(columns: List[String]): IO[Unit] =
      state.update(_ :+ AddedColumns(columns))
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

  private def testChannelProvider(ref: Ref[IO, Vector[Action]]): ChannelProvider[IO] = new ChannelProvider[IO] {
    def reset: IO[Unit] =
      ref.update(_ :+ ResetChannel)

    def insert(rows: Seq[Map[String, AnyRef]]): IO[Seq[ChannelProvider.InsertFailure]] =
      ref.update(_ :+ InsertedRows(rows.size)).as(Seq.empty)
  }

  def testMetrics(ref: Ref[IO, Vector[Action]]): Metrics[IO] = new Metrics[IO] {
    def addBad(count: Int): IO[Unit] =
      ref.update(_ :+ AddedBadCountMetric(count))

    def addGood(count: Int): IO[Unit] =
      ref.update(_ :+ AddedGoodCountMetric(count))

    def report: Stream[IO, Nothing] = Stream.never[IO]
  }
}
