/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.implicits._
import cats.{Applicative, Foldable, Monad, Semigroup}
import cats.effect.{Async, Sync}
import cats.effect.kernel.Unique
import fs2.{Pipe, Pull, Stream}
import net.snowflake.ingest.utils.{ErrorCode, SFException}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.nio.charset.StandardCharsets
import java.time.OffsetDateTime
import scala.concurrent.duration.Duration

import com.snowplowanalytics.iglu.schemaddl.parquet.Caster
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Payload => BadPayload, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.Payload.{RawPayload => BadRowRawPayload}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, TokenedEvents}
import com.snowplowanalytics.snowplow.snowflake.{Config, Environment}
import com.snowplowanalytics.snowplow.loaders.common.Transform

object Processing {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def stream[F[_]: Async](env: Environment[F]): Stream[F, Nothing] = {
    val eventProcessingConfig = EventProcessingConfig(EventProcessingConfig.NoWindowing)
    env.source.stream(eventProcessingConfig, eventProcessor(env))
  }

  /** Model used between stages of the processing pipeline */

  private case class ParsedBatch(
    events: List[Event],
    bad: List[BadRow],
    countBytes: Long,
    token: Unique.Token
  )

  type EventWithTransform = (Event, Map[String, AnyRef])

  /**
   * State of a batch for all stages post-transform
   *
   * @param toBeInserted
   *   Events from this batch which have not yet been inserted. Events are dropped from this list
   *   once they have either failed or got inserted. Implemented as a Vector because we need to do
   *   lookup by index.
   * @param origBatchSize
   *   The count of events in the original batch. Includes all good and bad events.
   * @param origBatchBytes
   *   The total size in bytes of events in the original batch. Includes all good and bad events.
   * @param badAccumulated
   *   Events that failed for any reason so far.
   * @param tokens
   *   The tokens to be emitted after we have finished processing all events
   */
  private case class BatchAfterTransform(
    toBeInserted: Vector[EventWithTransform],
    origBatchBytes: Long,
    badAccumulated: List[BadRow],
    countInserted: Int,
    tokens: List[Unique.Token]
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
    eventsWithExtraCols: Vector[EventWithTransform],
    unexpectedFailures: List[(Event, SFException)]
  )

  private object ParsedWriteResult {
    def empty: ParsedWriteResult = ParsedWriteResult(Set.empty, Vector.empty, Nil)

    def buildFrom(events: Vector[EventWithTransform], results: List[ChannelProvider.WriteFailure]): ParsedWriteResult =
      results.foldLeft(ParsedWriteResult.empty) { case (ParsedWriteResult(extraCols, eventsWithExtraCols, unexpected), failure) =>
        val event = fastGetByIndex(events, failure.index)
        if (failure.extraCols.nonEmpty)
          ParsedWriteResult(extraCols ++ failure.extraCols, eventsWithExtraCols :+ event, unexpected)
        else
          ParsedWriteResult(extraCols, eventsWithExtraCols, (event._1, failure.cause) :: unexpected)
      }
  }

  private def eventProcessor[F[_]: Async](
    env: Environment[F]
  ): EventProcessor[F] = { in =>
    val badProcessor = BadRowProcessor(env.appInfo.name, env.appInfo.version)

    in.through(parseBytes(badProcessor))
      .through(transform(badProcessor))
      .through(batchUp(env.batching))
      .through(writeToSnowflake(env, badProcessor))
      .through(sendFailedEvents(env))
      .through(sendMetrics(env))
      .through(emitTokens)
  }

  /** Parse raw bytes into Event using analytics sdk */
  private def parseBytes[F[_]: Monad](badProcessor: BadRowProcessor): Pipe[F, TokenedEvents, ParsedBatch] =
    _.evalMap { case TokenedEvents(list, token, _) =>
      Foldable[List].foldM(list, ParsedBatch(Nil, Nil, 0L, token)) { case (acc, bytes) =>
        Applicative[F].pure {
          val bytesSize   = bytes.capacity
          val stringified = StandardCharsets.UTF_8.decode(bytes).toString
          Event.parse(stringified).toEither match {
            case Right(e) =>
              acc.copy(events = e :: acc.events, countBytes = acc.countBytes + bytesSize)
            case Left(failure) =>
              val payload = BadRowRawPayload(stringified)
              val bad     = BadRow.LoaderParsingError(badProcessor, failure, payload)
              acc.copy(bad = bad :: acc.bad, countBytes = acc.countBytes + bytesSize)
          }
        }
      }
    }

  /** Transform the Event into values compatible with the snowflake ingest sdk */
  private def transform[F[_]: Sync](badProcessor: BadRowProcessor): Pipe[F, ParsedBatch, BatchAfterTransform] =
    in =>
      for {
        ParsedBatch(events, bad, bytes, token) <- in
        loadTstamp <- Stream.eval(Sync[F].realTimeInstant).map(SnowflakeCaster.timestampValue)
        result <- Stream.eval(transformBatch[F](badProcessor, events, loadTstamp))
        (moreBad, transformed) = result.separate
      } yield BatchAfterTransform(
        toBeInserted   = transformed.toVector,
        origBatchBytes = bytes,
        badAccumulated = bad ::: moreBad,
        countInserted  = 0,
        tokens         = List(token)
      )

  private def transformBatch[F[_]: Monad](
    badProcessor: BadRowProcessor,
    events: List[Event],
    loadTstamp: OffsetDateTime
  ): F[List[Either[BadRow, (Event, Map[String, AnyRef])]]] =
    events
      .traverse { e =>
        Applicative[F].pure {
          Transform
            .transformEventUnstructured[AnyRef](badProcessor, SnowflakeCaster, SnowflakeJsonFolder, e)
            .map { namedValues =>
              val asMap = namedValues.map { case Caster.NamedValue(k, v) =>
                k -> v
              }.toMap
              (e, asMap + ("load_tstamp" -> loadTstamp))
            }
        }
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
    handleFailures: List[ChannelProvider.WriteFailure] => F[BatchAfterTransform]
  ): F[BatchAfterTransform] =
    if (batch.toBeInserted.isEmpty)
      batch.pure[F]
    else
      Sync[F].untilDefinedM {
        env.channelProvider.write(batch.toBeInserted.map(_._2)).flatMap {
          case ChannelProvider.WriteResult.ChannelIsInvalid =>
            // Reset the channel and immediately try again
            env.channelProvider.reset.as(none)
          case ChannelProvider.WriteResult.WriteFailures(notWritten) =>
            handleFailures(notWritten).map(Some(_))
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
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(env, batch) { notWritten =>
      val parsedResult = ParsedWriteResult.buildFrom(batch.toBeInserted, notWritten)
      for {
        _ <- abortIfFatalException[F](parsedResult.unexpectedFailures)
        _ <- handleSchemaEvolution(env, parsedResult.extraColsRequired)
      } yield {
        val moreBad = parsedResult.unexpectedFailures.map { case (event, sfe) =>
          badRowFromEnqueueFailure(badProcessor, event, sfe)
        }
        batch.copy(
          toBeInserted   = parsedResult.eventsWithExtraCols,
          badAccumulated = moreBad ::: batch.badAccumulated,
          countInserted  = batch.toBeInserted.size - notWritten.size
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
      val mapped = notWritten.map(f => (fastGetByIndex(batch.toBeInserted, f.index)._1, f.cause))
      abortIfFatalException[F](mapped).as {
        val moreBad = mapped.map { case (event, sfe) =>
          badRowFromEnqueueFailure(badProcessor, event, sfe)
        }
        batch.copy(
          toBeInserted   = Vector.empty,
          badAccumulated = moreBad ::: batch.badAccumulated,
          countInserted  = batch.countInserted + batch.toBeInserted.size - notWritten.size
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
  private def handleSchemaEvolution[F[_]: Monad](
    env: Environment[F],
    extraColsRequired: Set[String]
  ): F[Unit] =
    if (extraColsRequired.isEmpty)
      ().pure[F]
    else
      env.channelProvider.withClosedChannel {
        env.tblManager.addColumns(extraColsRequired.toList)
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
      env.metrics.addGood(batch.countInserted) *> env.metrics.addBad(batch.badAccumulated.size)
    }

  private def emitTokens[F[_]]: Pipe[F, BatchAfterTransform, Unique.Token] =
    _.flatMap { batch =>
      Stream.emits(batch.tokens)
    }

  private def fastGetByIndex[A](items: Vector[A], index: Long): A = items(index.toInt)

  private implicit def batchedSemigroup: Semigroup[BatchAfterTransform] = new Semigroup[BatchAfterTransform] {
    def combine(x: BatchAfterTransform, y: BatchAfterTransform): BatchAfterTransform =
      BatchAfterTransform(
        toBeInserted   = x.toBeInserted ++ y.toBeInserted,
        origBatchBytes = x.origBatchBytes + y.origBatchBytes,
        badAccumulated = x.badAccumulated ::: y.badAccumulated,
        countInserted  = x.countInserted + y.countInserted,
        tokens         = x.tokens ::: y.tokens
      )
  }

  private def batchUp[F[_]: Async](config: Config.Batching): Pipe[F, BatchAfterTransform, BatchAfterTransform] = {
    def go(
      timedPull: Pull.Timed[F, BatchAfterTransform],
      unflushed: Option[BatchAfterTransform]
    ): Pull[F, BatchAfterTransform, Unit] =
      timedPull.uncons.flatMap {
        case None => // Upstream stream has finished cleanly
          unflushed match {
            case None    => Pull.done
            case Some(b) => Pull.output1(b) *> Pull.done
          }
        case Some((Left(_), next)) => // The timer we set has timed out.
          unflushed match {
            case None    => go(next, None)
            case Some(b) => Pull.output1(b) >> go(next, None)
          }
        case Some((Right(pulled), next)) if pulled.isEmpty =>
          go(next, unflushed)
        case Some((Right(nonEmptyChunk), next)) => // Received another batch before the timer timed out
          val combined = unflushed match {
            case None    => nonEmptyChunk.iterator.reduce(_ |+| _)
            case Some(b) => nonEmptyChunk.iterator.foldLeft(b)(_ |+| _)
          }
          if (combined.origBatchBytes > config.maxBytes)
            for {
              _ <- Pull.output1(combined)
              _ <- next.timeout(Duration.Zero)
              _ <- go(next, None)
            } yield ()
          else {
            for {
              _ <- if (unflushed.isEmpty) next.timeout(config.maxDelay) else Pull.pure(())
              _ <- go(next, Some(combined))
            } yield ()
          }
      }
    in =>
      in.pull.timed { timedPull =>
        go(timedPull, None)
      }.stream
  }

}
