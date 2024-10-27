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

import cats.implicits._
import cats.{Applicative, Foldable}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import com.snowplowanalytics.iglu.core.SchemaCriterion
import fs2.{Chunk, Pipe, Stream}
import net.snowflake.ingest.utils.{ErrorCode, SFException}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import java.time.{Instant, OffsetDateTime}
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.snowflake.{Environment, Metrics, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, Transform}

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    val eventProcessingConfig = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    Stream.eval(env.tableManager.initializeEventsTable()) *>
      Stream.eval(env.channel.opened.use_) *>
      env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParsedBatch(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    countItems: Int,
    token: Unique.Token
  )

  private case class TransformedBatch(
    events: List[EventWithTransform],
    parseFailures: List[BadRow],
    transformFailures: List[BadRow],
    countBytes: Long,
    countItems: Int,
    token: Unique.Token
  )

  type EventWithTransform = (Event, Map[String, AnyRef])

  /**
   * State of a batch for all stages post-transform
   *
   * @param toBeInserted
   *   Events from this batch which have not yet been inserted. Events are dropped from this list
   *   once they have either failed or got inserted.
   * @param origBatchBytes
   *   The total size in bytes of events in the original batch. Includes all good and bad events.
   * @param origBatchCount
   *   The count of events in the original batch. Includes all good and bad events.
   * @param badAccumulated
   *   Events that failed for any reason so far.
   * @param tokens
   *   The tokens to be emitted after we have finished processing all events
   */
  private case class BatchAfterTransform(
    toBeInserted: ListOfList[EventWithTransform],
    origBatchBytes: Long,
    origBatchCount: Int,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token],
    maxCollectorTstamp: Option[Instant],
    minCollectorTstamp: Option[Instant]
  )

  /**
   * Result of attempting to enqueue a batch of events to be sent to Snowflake
   *
   * @param extraCols
   *   The column names which were present in the batch but missing in the table
   * @param eventsWithExtraCols
   *   Events which failed to be inserted because they contained extra columns are missing in the
   *   table. These issues should be resolved once we alter the table.
   * @param unexpectedFailures
   *   Events which failed to be inserted for any other reason
   */
  private case class ParsedWriteResult(
    extraColsRequired: Set[String],
    eventsWithExtraCols: List[EventWithTransform],
    unexpectedFailures: List[(Event, SFException)]
  )

  private object ParsedWriteResult {
    def empty: ParsedWriteResult = ParsedWriteResult(Set.empty, Nil, Nil)

    def buildFrom(events: ListOfList[EventWithTransform], writeFailures: List[Channel.WriteFailure]): ParsedWriteResult =
      if (writeFailures.isEmpty)
        empty
      else {
        val indexed = events.copyToIndexedSeq
        writeFailures.foldLeft(ParsedWriteResult.empty) { case (ParsedWriteResult(extraCols, eventsWithExtraCols, unexpected), failure) =>
          val event = fastGetByIndex(indexed, failure.index)
          if (failure.extraCols.nonEmpty)
            ParsedWriteResult(extraCols ++ failure.extraCols, event :: eventsWithExtraCols, unexpected)
          else
            ParsedWriteResult(extraCols, eventsWithExtraCols, (event._1, failure.cause) :: unexpected)
        }
      }
  }

  private def eventProcessor[F[_]: Async](env: Environment[F]): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(setLatency(env.metrics))
      .through(parseBytes(badProcessor))
      .through(addOutstandingBatchToMetrics(env.metrics))
      .through(transform(badProcessor, env.schemasToSkip))
      .through(BatchUp.withTimeout(env.batching.maxBytes, env.batching.maxDelay))
      .through(writeToSnowflake(env, badProcessor))
      .through(sendFailedEvents(env, badProcessor))
      .through(sendMetrics(env))
      .through(emitTokens)
      .through(clearOutstandingBatchFromMetrics(env.metrics))
  }

  private def setLatency[F[_]: Sync](metrics: Metrics[F]): Pipe[F, TokenedEvents, TokenedEvents] =
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

  private def addOutstandingBatchToMetrics[F[_]: Sync](metrics: Metrics[F]): Pipe[F, ParsedBatch, ParsedBatch] =
    _.evalTap { parsedBatch =>
      parsedBatch.events.map(_.collector_tstamp).maxOption match {
        case Some(maxCollectorTstamp) =>
          metrics.addOutstandingBatch(parsedBatch.token, maxCollectorTstamp)
        case None =>
          Sync[F].unit
      }
    }

  private def clearOutstandingBatchFromMetrics[F[_]: Sync](metrics: Metrics[F]): Pipe[F, Unique.Token, Unique.Token] =
    _.evalTapChunk(metrics.clearOutstandingBatch(_))

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Sync](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParsedBatch] =
    _.evalMap { case TokenedEvents(chunk, token, _) =>
      for {
        numBytes <- Sync[F].delay(Foldable[Chunk].sumBytes(chunk))
        (badRows, events) <- Foldable[Chunk].traverseSeparateUnordered(chunk) { bytes =>
                               Sync[F].delay {
                                 Event.parseBytes(bytes).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(bytes).toString)
                                   BadRow.LoaderParsingError(badProcessor, failure, payload)
                                 }
                               }
                             }
      } yield ParsedBatch(events, badRows, numBytes, chunk.size, token)
    }

  /** Transform the Event into values compatible with the snowflake ingest sdk */
  private def transform[F[_]: Sync](
    badProcessor: BadRowProcessor,
    schemasToSkip: List[SchemaCriterion]
  ): Pipe[F, ParsedBatch, TransformedBatch] =
    _.evalMap { batch =>
      Sync[F].realTimeInstant.flatMap { now =>
        val loadTstamp = SnowflakeCaster.timestampValue(now)
        transformBatch[F](badProcessor, loadTstamp, batch, schemasToSkip)
      }
    }

  private def transformBatch[F[_]: Sync](
    badProcessor: BadRowProcessor,
    loadTstamp: OffsetDateTime,
    batch: ParsedBatch,
    schemasToSkip: List[SchemaCriterion]
  ): F[TransformedBatch] =
    Foldable[List]
      .traverseSeparateUnordered(batch.events) { event =>
        Sync[F].delay {
          Transform
            .transformEventUnstructured[AnyRef](badProcessor, SnowflakeCaster, SnowflakeJsonFolder, event, schemasToSkip)
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

  private def writeToSnowflake[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.parEvalMap(env.batching.uploadConcurrency) { batch =>
      for {
        batch <- writeAttempt1(env, badProcessor, batch)
        batch <- writeAttempt2(env, badProcessor, batch)
      } yield batch
    }

  private def withWriteAttempt[F[_]: Sync](
    env: Environment[F],
    batch: BatchAfterTransform
  )(
    handleFailures: List[Channel.WriteFailure] => F[BatchAfterTransform]
  ): F[BatchAfterTransform] = {
    val attempt: F[BatchAfterTransform] =
      if (batch.toBeInserted.isEmpty)
        batch.pure[F]
      else
        Sync[F].untilDefinedM {
          env.channel.opened
            .use { channel =>
              channel.write(batch.toBeInserted.asIterable.map(_._2))
            }
            .flatMap {
              case Channel.WriteResult.ChannelIsInvalid =>
                // Reset the channel and immediately try again
                env.channel.closed.use_.as(none)
              case Channel.WriteResult.WriteFailures(notWritten) =>
                handleFailures(notWritten).map(Some(_))
            }
        }

    attempt
      .onError { _ =>
        env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Snowflake)
      }
  }

  private def setCollectorToTargetLatencyMetrics[F[_]: Sync](metrics: Metrics[F], batch: BatchAfterTransform): F[Unit] =
    (batch.maxCollectorTstamp, batch.minCollectorTstamp)
      .mapN { (maxCT, minCT) =>
        for {
          now <- Sync[F].realTime
          optimistic  = now.toMillis - maxCT.toEpochMilli
          pessimistic = now.toMillis - minCT.toEpochMilli
          _ <- metrics.setLatencyCollectorToTargetMillis(optimistic)
          _ <- metrics.setLatencyCollectorToTargetPessimisticMillis(pessimistic)
        } yield ()
      }
      .fold(Sync[F].unit)(identity)

  /**
   * First attempt to write events with the Snowflake SDK
   *
   * Enqueue failures are expected if the Event contains columns which are not present in the target
   * table. If this happens, we alter the table ready for the second attempt
   */
  private def writeAttempt1[F[_]: Sync](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(env, batch) { notWritten =>
      val setLatency =
        // best case branch 1 - all events are inserted
        if (notWritten.isEmpty) setCollectorToTargetLatencyMetrics(env.metrics, batch)
        else Sync[F].unit
      val parsedResult = ParsedWriteResult.buildFrom(batch.toBeInserted, notWritten)
      for {
        _ <- setLatency
        _ <- abortIfFatalException[F](parsedResult.unexpectedFailures)
        _ <- handleSchemaEvolution(env, parsedResult.extraColsRequired)
      } yield {
        val moreBad = parsedResult.unexpectedFailures.map { case (event, sfe) =>
          badRowFromEnqueueFailure(badProcessor, event, sfe)
        }
        batch.copy(
          toBeInserted   = ListOfList.ofLists(parsedResult.eventsWithExtraCols),
          badAccumulated = batch.badAccumulated.prepend(moreBad)
        )
      }
    }

  /**
   * Second attempt to write events with the Snowflake SDK
   *
   * This happens after we have attempted to alter the table for any new columns. So insert errors
   * at this stage are unexpected.
   */
  private def writeAttempt2[F[_]: Sync](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(env, batch) { notWritten =>
      val setLatency =
        // best case branch 2 - all remaining events left from attempt 1 are inserted
        if (notWritten.isEmpty) setCollectorToTargetLatencyMetrics(env.metrics, batch)
        else Sync[F].unit
      val mapped = notWritten match {
        case Nil => Nil
        case more =>
          val indexed = batch.toBeInserted.copyToIndexedSeq
          more.map(f => (fastGetByIndex(indexed, f.index)._1, f.cause))
      }
      setLatency >> abortIfFatalException[F](mapped).as {
        val moreBad = mapped.map { case (event, sfe) =>
          badRowFromEnqueueFailure(badProcessor, event, sfe)
        }
        batch.copy(
          toBeInserted   = ListOfList.empty,
          badAccumulated = batch.badAccumulated.prepend(moreBad)
        )
      }
    }

  private def badRowFromEnqueueFailure(
    badProcessor: BadRowProcessor,
    event: Event,
    cause: SFException
  ): BadRow =
    BadRow.LoaderRuntimeError(badProcessor, cause.getMessage, BadPayload.LoaderPayload(event))

  /**
   * The sub-set of vendor codes that indicate a problem with *data* rather than problems with the
   * environment
   */
  private val dataIssueVendorCodes: Set[String] =
    List(
      ErrorCode.INVALID_VALUE_ROW,
      ErrorCode.INVALID_FORMAT_ROW,
      ErrorCode.MAX_ROW_SIZE_EXCEEDED,
      ErrorCode.UNKNOWN_DATA_TYPE,
      ErrorCode.NULL_VALUE,
      ErrorCode.NULL_OR_EMPTY_STRING
    ).map(_.getMessageCode).toSet

  /**
   * Raises an exception if needed
   *
   * The Snowflake SDK returns *all* exceptions as though they are equal. But we want to treat them
   * separately:
   *   - Problems with data should be handled as Failed Events
   *   - Runtime problems (e.g. network issue or closed channel) should halt processing, so we don't
   *     send all events to the bad topic.
   */
  private def abortIfFatalException[F[_]: Sync](results: List[(Event, SFException)]): F[Unit] =
    results.traverse_ { case (_, sfe) =>
      if (dataIssueVendorCodes.contains(sfe.getVendorCode))
        Sync[F].unit
      else
        Logger[F].error(sfe)("Insert yielded an error which this app cannot tolerate") *>
          Sync[F].raiseError[Unit](sfe)
    }

  /**
   * Alters the table to add any columns that were present in the Events but not currently in the
   * table
   */
  private def handleSchemaEvolution[F[_]: Sync](
    env: Environment[F],
    extraColsRequired: Set[String]
  ): F[Unit] =
    if (extraColsRequired.isEmpty)
      ().pure[F]
    else
      env.channel.closed.surround {
        env.tableManager.addColumns(extraColsRequired.toList)
      }

  private def sendFailedEvents[F[_]: Sync](
    env: Environment[F],
    badRowProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      if (batch.badAccumulated.nonEmpty) {
        val serialized =
          batch.badAccumulated.mapUnordered(badRow => BadRowsSerializer.withMaxSize(badRow, badRowProcessor, env.badRowMaxSize))
        env.badSink
          .sinkSimple(serialized)
          .onError { _ =>
            env.appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)
          }
      } else Applicative[F].unit
    }

  private def sendMetrics[F[_]: Applicative](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap { batch =>
      val countBad = batch.badAccumulated.asIterable.size
      env.metrics.addGood(batch.origBatchCount - countBad) *> env.metrics.addBad(countBad)
    }

  private def emitTokens[F[_]]: Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private def fastGetByIndex[A](items: IndexedSeq[A], index: Long): A = items(index.toInt)

  private implicit class MaxCollectorTstamp(events: List[EventWithTransform]) {
    def maxCollectorTstamp(): Option[Instant] = events.map(_._1).map(_.collector_tstamp).maxOption
  }

  private implicit class MinCollectorTstamp(events: List[EventWithTransform]) {
    def minCollectorTstamp(): Option[Instant] = events.map(_._1).map(_.collector_tstamp).minOption
  }

  private implicit def batchable: BatchUp.Batchable[TransformedBatch, BatchAfterTransform] =
    new BatchUp.Batchable[TransformedBatch, BatchAfterTransform] {
      def combine(b: BatchAfterTransform, a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          toBeInserted       = b.toBeInserted.prepend(a.events),
          origBatchBytes     = b.origBatchBytes + a.countBytes,
          origBatchCount     = b.origBatchCount + a.countItems,
          badAccumulated     = b.badAccumulated.prepend(a.parseFailures).prepend(a.transformFailures),
          tokens             = b.tokens :+ a.token,
          maxCollectorTstamp = List(b.maxCollectorTstamp, a.events.maxCollectorTstamp()).max,
          minCollectorTstamp = List(b.maxCollectorTstamp, a.events.maxCollectorTstamp()).min
        )

      def single(a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          ListOfList.of(List(a.events)),
          a.countBytes,
          a.countItems,
          ListOfList.ofLists(a.parseFailures, a.transformFailures),
          Vector(a.token),
          a.events.maxCollectorTstamp(),
          a.events.minCollectorTstamp()
        )

      def weightOf(a: TransformedBatch): Long =
        a.countBytes
    }

}
