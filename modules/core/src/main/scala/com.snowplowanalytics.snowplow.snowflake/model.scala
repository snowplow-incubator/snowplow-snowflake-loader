package com.snowplowanalytics.snowplow.snowflake

import cats.effect.kernel.Unique
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.badrows.BadRow
import com.snowplowanalytics.snowplow.runtime.processing.BatchUp
import com.snowplowanalytics.snowplow.sinks.ListOfList
import com.snowplowanalytics.snowplow.snowflake.processing.Channel
import net.snowflake.ingest.utils.SFException

object model {

  /** Model used between stages of the processing pipeline */

  case class ParsedBatch(
    events: List[Event],
    parseFailures: List[BadRow],
    countBytes: Long,
    countItems: Int,
    token: Unique.Token
  )

  case class TransformedBatch(
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
  case class BatchAfterTransform(
    toBeInserted: ListOfList[EventWithTransform],
    origBatchBytes: Long,
    origBatchCount: Int,
    badAccumulated: ListOfList[BadRow],
    tokens: Vector[Unique.Token]
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
  case class ParsedWriteResult(
    extraColsRequired: Set[String],
    eventsWithExtraCols: List[EventWithTransform],
    unexpectedFailures: List[(Event, SFException)]
  )

  object ParsedWriteResult {
    def empty: ParsedWriteResult = ParsedWriteResult(Set.empty, Nil, Nil)

    def buildFrom(events: ListOfList[EventWithTransform], writeFailures: List[Channel.WriteFailure]): ParsedWriteResult =
      if (writeFailures.isEmpty)
        empty
      else {
        val indexed = events.copyToIndexedSeq
        writeFailures.foldLeft(ParsedWriteResult.empty) { case (ParsedWriteResult(extraCols, eventsWithExtraCols, unexpected), failure) =>
          val event = indexed(failure.index.toInt)
          if (failure.extraCols.nonEmpty)
            ParsedWriteResult(extraCols ++ failure.extraCols, event :: eventsWithExtraCols, unexpected)
          else
            ParsedWriteResult(extraCols, eventsWithExtraCols, (event._1, failure.cause) :: unexpected)
        }
      }
  }

  implicit def batchable: BatchUp.Batchable[TransformedBatch, BatchAfterTransform] =
    new BatchUp.Batchable[TransformedBatch, BatchAfterTransform] {
      def combine(b: BatchAfterTransform, a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          toBeInserted   = b.toBeInserted.prepend(a.events),
          origBatchBytes = b.origBatchBytes + a.countBytes,
          origBatchCount = b.origBatchCount + a.countItems,
          badAccumulated = b.badAccumulated.prepend(a.parseFailures).prepend(a.transformFailures),
          tokens         = b.tokens :+ a.token
        )

      def single(a: TransformedBatch): BatchAfterTransform =
        BatchAfterTransform(
          ListOfList.of(List(a.events)),
          a.countBytes,
          a.countItems,
          ListOfList.ofLists(a.parseFailures, a.transformFailures),
          Vector(a.token)
        )

      def weightOf(a: TransformedBatch): Long =
        a.countBytes
    }
}
