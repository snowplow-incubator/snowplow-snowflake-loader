/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.implicits._
import cats.{Applicative, Monad}
import cats.effect.Async
import cats.effect.kernel.Unique
import fs2.{Pipe, Stream}

import java.nio.charset.StandardCharsets

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.snowflake.Environment
import com.snowplowanalytics.snowplow.loaders.Transform

object Processing {

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    val eventProcessingConfig = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParsedBatch(
    events: List[Event],
    bad: List[BadRow],
    token: Unique.Token
  )

  /**
   * State of a batch for all stages post-transform
   *
   * @param toBeInserted
   *   Events from this batch which have not yet been inserted. Events are dropped from this list
   *   once they have either failed or got inserted. Implemented as a Vector because we need to do
   *   lookup by index.
   * @param origBatchSize
   *   The count of events in the original batch. Includes all good and bad events.
   * @param badAccumulated
   *   Events that failed for any reason so far.
   * @param token
   *   The token to be emitted after we have finished processing all events
   */
  private case class BatchAfterTransform(
    toBeInserted: Vector[(Event, Map[String, AnyRef])],
    origBatchSize: Int,
    badAccumulated: List[BadRow],
    token: Unique.Token
  )

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(parseBytes(badProcessor))
      .through(transform(badProcessor))
      .through(sinkAttempt1(env, badProcessor))
      .through(sinkAttempt2(env, badProcessor))
      .through(sendFailedEvents(env))
      .through(sendMetrics(env))
      .map(_.token)
  }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Monad](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParsedBatch] =
    _.evalMap { case TokenedEvents(list, token) =>
      list
        .traverse { bytes =>
          Applicative[F].pure {
            val stringified = new String(bytes, StandardCharsets.UTF_8)
            Event
              .parse(stringified)
              .leftMap { failure =>
                val payload = BadRowRawPayload(stringified)
                BadRow.LoaderParsingError(badProcessor, failure, payload)
              }
          }
        }
        .flatMap { result =>
          Applicative[F].pure(result.separate)
        }
        .map { case (bad, good) =>
          ParsedBatch(good, bad, token)
        }
    }

  /** Transform the Event into values compatible with the snowflake ingest sdk */
  private def transform[F[_]: Monad](badProcessor: BadRowProcessor): Pipe[F, ParsedBatch, BatchAfterTransform] =
    _.evalMap { case ParsedBatch(events, bad, token) =>
      events
        .traverse { e =>
          Applicative[F].pure {
            Transform
              .transformEventUnstructured[AnyRef](badProcessor, SnowflakeCaster, SnowflakeJsonFolder, e)
              .map { namedValues =>
                val asMap = namedValues.map { case Caster.NamedValue(k, v) =>
                  k -> v
                }.toMap
                (e, asMap)
              }
          }
        }
        .flatMap { result =>
          Applicative[F].pure(result.separate)
        }
        .map { case (moreBad, transformed) =>
          BatchAfterTransform(transformed.toVector, events.size + bad.size, bad ::: moreBad, token)
        }
    }

  /**
   * First attempt to write events to Snowflake
   *
   * Insert failures are expected if the Event contains columns which are not present in the target
   * table. If this happens, we alter the table ready for the second attempt
   */
  private def sinkAttempt1[F[_]: Monad](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalMap { case batch @ BatchAfterTransform(events, _, bad, _) =>
      if (events.isEmpty)
        batch.pure[F]
      else
        for {
          notInserted <- env.channelProvider.insert(events.map(_._2))
          (reqExtraCols, otherFails) = separateFailures(notInserted)
          _ <- handleSchemaEvolution(env, reqExtraCols)
        } yield {
          val toBeRetried = reqExtraCols.map(rec => fastGetByIndex(events, rec.index)).toVector
          val moreBad = otherFails.map { of =>
            val event = fastGetByIndex(events, of.index)
            BadRow.LoaderRuntimeError(badProcessor, of.reason, BadPayload.LoaderPayload(event._1))
          }
          batch.copy(toBeInserted = toBeRetried, badAccumulated = moreBad ::: bad)
        }
    }

  /**
   * Second attempt to write events to Snowflake
   *
   * This happens after we have attempted to alter the table for any new columns. So insert errors
   * at this stage are unexpected.
   */
  private def sinkAttempt2[F[_]: Monad](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalMap { case batch @ BatchAfterTransform(events, _, bad, _) =>
      if (events.isEmpty)
        batch.pure[F]
      else
        for {
          notInserted <- env.channelProvider.insert(events.map(_._2))
        } yield {
          val moreBad = notInserted.map {
            case ChannelProvider.UnexpectedFailure(index, reason) =>
              val event = fastGetByIndex(events, index)
              BadRow.LoaderRuntimeError(badProcessor, reason, BadPayload.LoaderPayload(event._1))
            case ChannelProvider.RequiresExtraColumns(index, extraCols) =>
              // This is an illegal state.  We have already altered the table, so it is unexpected for
              // a failure to happen because of extra columns in the event.
              val event = fastGetByIndex(events, index)
              val missing = extraCols.mkString(", ")
              BadRow.LoaderRuntimeError(badProcessor, s"Table missing columns: $missing", BadPayload.LoaderPayload(event._1))
          }
          batch.copy(toBeInserted = Vector.empty, badAccumulated = moreBad.toList ::: bad)
        }
    }

  private def separateFailures(
    insertFailures: Seq[ChannelProvider.InsertFailure]
  ): (List[ChannelProvider.RequiresExtraColumns], List[ChannelProvider.UnexpectedFailure]) =
    insertFailures.foldLeft(
      (List.empty[ChannelProvider.RequiresExtraColumns], List.empty[ChannelProvider.UnexpectedFailure])
    ) {
      case ((accExtraCols, accUnexpected), extraCols: ChannelProvider.RequiresExtraColumns) =>
        (extraCols :: accExtraCols, accUnexpected)
      case ((accExtraCols, accUnexpected), unexpected: ChannelProvider.UnexpectedFailure) =>
        (accExtraCols, unexpected :: accUnexpected)
    }

  /**
   * Alters the table to add any columns that were present in the Events but not currently in the
   * table
   */
  private def handleSchemaEvolution[F[_]: Monad](
    env: Environment[F],
    insertFailures: List[ChannelProvider.RequiresExtraColumns]
  ): F[Unit] =
    if (insertFailures.isEmpty)
      ().pure[F]
    else {
      val extraCols = insertFailures.foldLeft(Set.empty[String])(_ ++ _.extraCols)
      for {
        _ <- env.tblManager.addColumns(extraCols.toList)
        _ <- env.channelProvider.reset
      } yield ()
    }

  private def sendFailedEvents[F[_]: Applicative, A](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized = batch.badAccumulated.map(_.compact.getBytes(StandardCharsets.UTF_8))
        env.badSink.sinkSimple(serialized)
      } else Applicative[F].unit
    }

  private def sendMetrics[F[_]: Applicative, A](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      val countBad = batch.badAccumulated.size
      env.metrics.addGood(batch.origBatchSize - countBad) *> env.metrics.addBad(countBad)
    }

  private def fastGetByIndex[A](items: Vector[A], index: Long): A = items(index.toInt)

}
