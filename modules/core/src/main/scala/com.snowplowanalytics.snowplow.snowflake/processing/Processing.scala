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

import cats.data.NonEmptyList
import cats.implicits._
import cats.{Applicative, Foldable}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import com.snowplowanalytics.iglu.core.SchemaCriterion
import fs2.{Pipe, Stream}
import net.snowflake.ingest.utils.{ErrorCode, SFException}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationLong
import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import java.time.Instant

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, ParsingError}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.streams.{EventProcessingConfig, ListOfList}
import com.snowplowanalytics.snowplow.streams.compression.Decompression._
import com.snowplowanalytics.snowplow.snowflake.{Environment, RuntimeService}
import com.snowplowanalytics.snowplow.runtime.syntax.foldable._
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.loaders.transform.{BadRowsSerializer, Transform}

object Processing {

  private implicit def logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    val eventProcessingConfig = EventProcessingConfig(EventProcessingConfig.NoWindowing, env.metrics.setLatency)
    val badProcessor          = BadRowProcessor(env.appInfo.name, env.appInfo.version)
    Stream.eval(env.tableManager.initializeEventsTable()) *>
      Stream.eval(env.channels.head.opened.use_) *>
      env.source.decompressedStream(
        eventProcessingConfig,
        env.decompression,
        eventProcessor(env, badProcessor),
        badProcessor,
        toBadRow(badProcessor)
      )
  }

  /** Model used between stages of the processing pipeline */

  private case class TransformedBatch(
    events: List[EventWithTransform],
    parseFailures: List[BadRow],
    transformFailures: List[BadRow],
    countBytes: Long,
    countItems: Long,
    token: Option[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
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
    origBatchCount: Long,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token],
    earliestCollectorTstamp: Option[Instant]
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
    private def empty: ParsedWriteResult = ParsedWriteResult(Set.empty, Nil, Nil)

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

  private def eventProcessor[F[_]: Async](env: Environment[F], badProcessor: BadRowProcessor): DecompressedEventProcessor[F] =
    _.through(parseAndTransform(env, badProcessor))
      .through(BatchUp.withTimeout(env.batching.maxBytes, env.batching.maxDelay))
      .through(writeToSnowflake(env, badProcessor))
      .through(setE2ELatencyMetric(env))
      .through(sendFailedEvents(env, badProcessor))
      .through(sendMetrics(env))
      .through(emitTokens)

  private def toBadRow(processor: BadRowProcessor): DecompressionError => BadRow.LoaderParsingError =
    err =>
      BadRow.LoaderParsingError(
        processor,
        ParsingError.RowDecodingError(
          NonEmptyList.of(ParsingError.RowDecodingErrorInfo.UnhandledRowDecodingError(err.message))
        ),
        BadRowRawPayload(err.payload)
      )

  private def setE2ELatencyMetric[F[_]: Sync](env: Environment[F]): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.evalTap {
      _.earliestCollectorTstamp match {
        case Some(t) =>
          for {
            now <- Sync[F].realTime
            e2eLatency = now - t.toEpochMilli.millis
            _ <- env.metrics.setE2ELatency(e2eLatency)
          } yield ()
        case None => Sync[F].unit
      }
    }

  private def parseAndTransform[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, DecompressedTokenedEvents, TransformedBatch] =
    _.parEvalMap(env.cpuParallelism) { result =>
      val payloads = result.payloads
      for {
        numBytes <- Sync[F].delay(payloads.foldLeft(0L)(_ + _.remaining()))
        (badRows, events) <- Foldable[List].traverseSeparateUnordered(payloads) { bytes =>
                               Sync[F].delay {
                                 Event.parseBytes(bytes).toEither.leftMap { failure =>
                                   val payload = BadRowRawPayload(StandardCharsets.UTF_8.decode(bytes).toString)
                                   BadRow.LoaderParsingError(badProcessor, failure, payload)
                                 }
                               }
                             }
        now <- Sync[F].realTimeInstant
        loadTstamp = SnowflakeCaster.timestampValue(now)
        (transformBad, transformed) <- transformBatch(badProcessor, loadTstamp, events, env.schemasToSkip)
        earliestCollectorTstamp = events.view.map(_.collector_tstamp).minOption
      } yield TransformedBatch(
        transformed,
        transformBad,
        badRows ::: result.bad,
        numBytes,
        payloads.size.toLong + result.bad.size,
        result.ack,
        earliestCollectorTstamp
      )
    }

  private def transformBatch[F[_]: Sync](
    badProcessor: BadRowProcessor,
    loadTstamp: OffsetDateTime,
    events: List[Event],
    schemasToSkip: List[SchemaCriterion]
  ): F[(List[BadRow], List[EventWithTransform])] =
    Foldable[List]
      .traverseSeparateUnordered(events) { event =>
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

  private def writeToSnowflake[F[_]: Async](
    env: Environment[F],
    badProcessor: BadRowProcessor
  ): Pipe[F, BatchAfterTransform, BatchAfterTransform] =
    _.zip(Stream.emits(env.channels).repeat)
      .parEvalMap(env.channels.length) { case (batch, channelProvider) =>
        for {
          batch <- writeAttempt1(env, badProcessor, channelProvider, batch)
          batch <- writeAttempt2(env, badProcessor, channelProvider, batch)
        } yield batch
      }

  private def withWriteAttempt[F[_]: Sync](
    env: Environment[F],
    channelProvider: Channel.Provider[F],
    batch: BatchAfterTransform
  )(
    handleFailures: List[Channel.WriteFailure] => F[BatchAfterTransform]
  ): F[BatchAfterTransform] = {
    val attempt: F[BatchAfterTransform] =
      if (batch.toBeInserted.isEmpty)
        batch.pure[F]
      else {
        val toBeInserted = batch.toBeInserted.asIterable.map(_._2).toVector
        Sync[F].untilDefinedM {
          channelProvider.opened
            .use { channel =>
              channel.write(toBeInserted)
            }
            .flatMap {
              case Channel.WriteResult.ChannelIsInvalid =>
                // Reset the channel and immediately try again
                channelProvider.closed.use_.as(none)
              case Channel.WriteResult.WriteFailures(notWritten) =>
                handleFailures(notWritten).map(Some(_))
            }
        }
      }

    attempt
      .onError { _ =>
        env.appHealth.beUnhealthyForRuntimeService(RuntimeService.Snowflake)
      }
  }

  /**
   * First attempt to write events with the Snowflake SDK
   *
   * Enqueue failures are expected if the Event contains columns which are not present in the target
   * table. If this happens, we alter the table ready for the second attempt
   */
  private def writeAttempt1[F[_]: Sync](
    env: Environment[F],
    badProcessor: BadRowProcessor,
    channelProvider: Channel.Provider[F],
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(env, channelProvider, batch) { notWritten =>
      val parsedResult = ParsedWriteResult.buildFrom(batch.toBeInserted, notWritten)
      for {
        _ <- abortIfFatalException[F](parsedResult.unexpectedFailures)
        _ <- handleSchemaEvolution(env, channelProvider, parsedResult.extraColsRequired)
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
    channelProvider: Channel.Provider[F],
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(env, channelProvider, batch) { notWritten =>
      val mapped = notWritten match {
        case Nil => Nil
        case more =>
          val indexed = batch.toBeInserted.copyToIndexedSeq
          more.map(f => (fastGetByIndex(indexed, f.index)._1, f.cause))
      }
      abortIfFatalException[F](mapped).as {
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
    channelProvider: Channel.Provider[F],
    extraColsRequired: Set[String]
  ): F[Unit] =
    if (extraColsRequired.isEmpty)
      ().pure[F]
    else
      channelProvider.closed.surround {
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
      env.metrics.addGood(batch.origBatchCount - batch.badAccumulated.size) *> env.metrics.addBad(batch.badAccumulated.size)
    }

  private def emitTokens[F[_]]: Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private def fastGetByIndex[A](items: IndexedSeq[A], index: Long): A = items(index.toInt)

  private implicit def batchable: BatchUp.Batchable[TransformedBatch, BatchAfterTransform] =
    new BatchUp.Batchable[TransformedBatch, BatchAfterTransform] {
      def combine(b: BatchAfterTransform, a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          toBeInserted   = b.toBeInserted.prepend(a.events),
          origBatchBytes = b.origBatchBytes + a.countBytes,
          origBatchCount = b.origBatchCount + a.countItems,
          badAccumulated = b.badAccumulated.prepend(a.parseFailures).prepend(a.transformFailures),
          tokens         = b.tokens :++ a.token,
          chooseEarliestTstamp(a.earliestCollectorTstamp, b.earliestCollectorTstamp)
        )

      def single(a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          ListOfList.of(List(a.events)),
          a.countBytes,
          a.countItems,
          ListOfList.ofLists(a.parseFailures, a.transformFailures),
          a.token.toVector,
          a.earliestCollectorTstamp
        )

      def weightOf(a: TransformedBatch): Long =
        a.countBytes
    }

  private def chooseEarliestTstamp(o1: Option[Instant], o2: Option[Instant]): Option[Instant] =
    (o1, o2)
      .mapN { case (t1, t2) =>
        if (t1.isBefore(t2)) t1 else t2
      }
      .orElse(o1)
      .orElse(o2)

}
