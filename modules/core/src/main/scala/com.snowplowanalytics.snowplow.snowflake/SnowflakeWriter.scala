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

import cats.effect.Sync
import cats.implicits._
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.{BadRow, Processor, Payload => BadPayload}
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.snowflake.model.{BatchAfterTransform, ParsedWriteResult}
import com.snowplowanalytics.snowplow.snowflake.processing.{Channel, TableManager}
import net.snowflake.ingest.utils.{ErrorCode, SFException}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final class SnowflakeWriter[F[_]: Sync](
  badProcessor: Processor,
  channel: Channel.Provider[F],
  tableManager: TableManager[F],
  appHealth: AppHealth[F]
) extends DestinationWriter[F] {

  private implicit def logger = Slf4jLogger.getLogger[F]

  def writeBatch(batch: BatchAfterTransform): F[BatchAfterTransform] =
    for {
      batch <- writeAttempt1(batch)
      batch <- writeAttempt2(batch)
    } yield batch

  /**
   * First attempt to write events with the Snowflake SDK
   *
   * Enqueue failures are expected if the Event contains columns which are not present in the target
   * table. If this happens, we alter the table ready for the second attempt
   */
  private def writeAttempt1(
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(batch) { notWritten =>
      val parsedResult = ParsedWriteResult.buildFrom(batch.toBeInserted, notWritten)
      for {
        _ <- abortIfFatalException(parsedResult.unexpectedFailures)
        _ <- handleSchemaEvolution(parsedResult.extraColsRequired)
      } yield {
        val moreBad = parsedResult.unexpectedFailures.map { case (event, sfe) =>
          badRowFromEnqueueFailure(event, sfe)
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
  private def writeAttempt2(
    batch: BatchAfterTransform
  ): F[BatchAfterTransform] =
    withWriteAttempt(batch) { notWritten =>
      val mapped = notWritten match {
        case Nil => Nil
        case more =>
          val indexed = batch.toBeInserted.copyToIndexedSeq
          more.map(f => (indexed(f.index.toInt)._1, f.cause))
      }
      abortIfFatalException(mapped).as {
        val moreBad = mapped.map { case (event, sfe) =>
          badRowFromEnqueueFailure(event, sfe)
        }
        batch.copy(
          toBeInserted   = ListOfList.empty,
          badAccumulated = batch.badAccumulated.prepend(moreBad)
        )
      }
    }

  private def withWriteAttempt(
    batch: BatchAfterTransform
  )(
    handleFailures: List[Channel.WriteFailure] => F[BatchAfterTransform]
  ): F[BatchAfterTransform] = {
    val attempt: F[BatchAfterTransform] =
      if (batch.toBeInserted.isEmpty)
        batch.pure[F]
      else
        Sync[F].untilDefinedM {
          channel.opened
            .use { channel =>
              channel.write(batch.toBeInserted.asIterable.map(_._2))
            }
            .flatMap {
              case Channel.WriteResult.ChannelIsInvalid =>
                // Reset the channel and immediately try again
                channel.closed.use_.as(none)
              case Channel.WriteResult.WriteFailures(notWritten) =>
                handleFailures(notWritten).map(Some(_))
            }
        }

    attempt
      .onError { _ =>
        appHealth.setServiceHealth(AppHealth.Service.Snowflake, isHealthy = false)
      }
  }

  /**
   * Raises an exception if needed
   *
   * The Snowflake SDK returns *all* exceptions as though they are equal. But we want to treat them
   * separately:
   *   - Problems with data should be handled as Failed Events
   *   - Runtime problems (e.g. network issue or closed channel) should halt processing, so we don't
   *     send all events to the bad topic.
   */
  private def abortIfFatalException(results: List[(Event, SFException)]): F[Unit] =
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
  private def handleSchemaEvolution(
    extraColsRequired: Set[String]
  ): F[Unit] =
    if (extraColsRequired.isEmpty)
      ().pure[F]
    else
      channel.closed.surround {
        tableManager.addColumns(extraColsRequired.toList)
      }

  private def badRowFromEnqueueFailure(
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
      ErrorCode.INVALID_FORMAT_ROW,
      ErrorCode.INVALID_VALUE_ROW,
      ErrorCode.MAX_ROW_SIZE_EXCEEDED,
      ErrorCode.UNKNOWN_DATA_TYPE,
      ErrorCode.NULL_VALUE,
      ErrorCode.NULL_OR_EMPTY_STRING
    ).map(_.getMessageCode).toSet

}
