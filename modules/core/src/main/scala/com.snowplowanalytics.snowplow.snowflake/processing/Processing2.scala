/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.kernel.Unique
import cats.effect.{Async, Sync}
import cats.implicits._
import cats.{Applicative, Foldable}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, Transform}
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.model._
import com.snowplowanalytics.snowplow.snowflake.{AppHealth, DestinationWriter, Metrics}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import fs2.{Chunk, Pipe, Stream}

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

final class Processing2[F[_]: Async](
  source: SourceAndAck[F],
  tableManager: TableManager[F],
  destinationWriter: DestinationWriter[F],
  badSink: Sink[F],
  appHealth: AppHealth[F],
  badRowProcessor: BadRowProcessor,
  metrics: Metrics[F],
  config: Processing2.Config
) {

  def stream: Stream[F, Nothing] = {
    val eventProcessingConfig = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    Stream.eval(tableManager.initializeEventsTable()) *>
      source.stream(eventProcessingConfig, eventProcessor())
  }

  private def eventProcessor(): EventProcessor[F] = { in =>
    in.through(setLatency())
      .through(parseBytes())
      .through(transform())
      .through(BatchUp.withTimeout(config.batchMaxWeight, config.batchMaxDelay))
      .through(writeToSnowflake())
      .through(sendFailedEvents())
      .through(sendMetrics())
      .through(emitTokens())
  }

  private def setLatency(): Pipe[F, TokenedEvents, TokenedEvents] =
    _.evalTap {
      _.earliestSourceTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            latencyMillis = now.toMillis - t.toEpochMilli
            _ <- metrics.setLatencyMillis(latencyMillis)
          } yield ()
        case None =>
          Applicative[F].unit
      }
    }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes(): Pipe[F, TokenedEvents, ParsedBatch] =
    _.evalMap { case TokenedEvents(chunk, token, _) =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { bytes =>
                               Sync[F].delay {
                                 Event.parseBytes(bytes).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(bytes).toString)
                                   BadRow.LoaderParsingError(badRowProcessor, failure, payload)
                                 }
                               }
                             }
      } yield ParsedBatch(events, badRows, numBytes, chunk.size, token)
    }

  /** Transform the Event into values compatible with the snowflake ingest sdk */
  private def transform(): Pipe[F, ParsedBatch, TransformedBatch] =
    _.evalMap { batch =>
      Sync[F].realTimeInstant.flatMap { now =>
        val loadTstamp = SnowflakeCaster.timestampValue(now)
        transformBatch(loadTstamp, batch)
      }
    }

  private def transformBatch(
    loadTstamp: OffsetDateTime,
    batch: ParsedBatch
  ): F[TransformedBatch] =
    Foldable[List]
      .traverseSeparateUnordered(batch.events) { event =>
        Sync[F].delay {
          Transform
            .transformEventUnstructured[AnyRef](badRowProcessor, SnowflakeCaster, SnowflakeJsonFolder, event, config.schemasToSkip)
            .map { namedValues =>
              val map = namedValues
                .map { case Caster.NamedValue(k, v) =>
                  k -> v
                }
                .toMap
                .updated("load_tstamp", loadTstamp)
              event -> map
            }
        }
      }
      .map { case (transformFailures, eventsWithTransforms) =>
        TransformedBatch(eventsWithTransforms, batch.parseFailures, transformFailures, batch.countBytes, batch.countItems, batch.token)
      }

  private def writeToSnowflake(): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.parEvalMap(config.uploadConcurrency) { batch =>
      destinationWriter.writeBatch(batch)
    }

  private def sendFailedEvents(): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized =
          batch.badAccumulated.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, badRowProcessor, config.badRowMaxSize))
        badSink
          .sinkSimple(serialized)
          .onError { _ =>
            appHealth.setServiceHealth(AppHealth.Service.BadSink, isHealthy = false)
          }
      } else Applicative[F].unit
    }

  private def sendMetrics(): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      val countBad = batch.badAccumulated.asIterable.size
      metrics.addGood(batch.origBatchCount - countBad) *> metrics.addBad(countBad)
    }

  private def emitTokens(): Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }
}

object Processing2 {

  final case class Config(
    schemasToSkip: List[SchemaCriterion],
    badRowMaxSize: Int,
    uploadConcurrency: Int,
    batchMaxWeight: Long,
    batchMaxDelay: FiniteDuration
  )
}
