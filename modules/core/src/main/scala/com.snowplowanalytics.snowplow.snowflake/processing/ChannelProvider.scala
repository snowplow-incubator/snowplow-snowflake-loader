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
import cats.effect.implicits._
import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Resource}
import cats.effect.std.{Hotswap, Semaphore}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import net.snowflake.ingest.streaming.{
  InsertValidationResponse,
  OpenChannelRequest,
  SnowflakeStreamingIngestChannel,
  SnowflakeStreamingIngestClient,
  SnowflakeStreamingIngestClientFactory
}
import net.snowflake.ingest.streaming.internal.SnowsFlakePlowInterop
import net.snowflake.ingest.utils.{ErrorCode => SFErrorCode, ParameterProvider, SFException}

import com.snowplowanalytics.snowplow.snowflake.Config

import java.time.ZoneOffset
import java.util.Properties
import scala.jdk.CollectionConverters._

trait ChannelProvider[F[_]] {

  /**
   * Closes the open channel and opens a new channel
   *
   * This should be called if the channel becomes invalid. And the channel becomes invalid if the
   * table is altered by another concurrent loader.
   */
  def reset: F[Unit]

  /**
   * Wraps an action which requires the channel to be closed
   *
   * This should be called when altering the table to add new columns. The newly opened channel will
   * be able to use the new columns.
   */
  def withClosedChannel[A](fa: F[A]): F[A]

  /**
   * Writes rows to Snowflake
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def write(rows: Iterable[Map[String, AnyRef]]): F[ChannelProvider.WriteResult]
}

object ChannelProvider {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
   * The result of trying to enqueue an event for sending to Snowflake
   * @param index
   *   Refers to the row number in the batch of attempted events
   * @param extraCols
   *   The column names which were present in the batch but missing in the table
   * @param cause
   *   The Snowflake exception, whose error code and message describes the reason for the failed
   *   enqueue
   */
  case class WriteFailure(
    index: Long,
    extraCols: List[String],
    cause: SFException
  )

  /** The result of trying to write a batch of events to Snowflake */
  sealed trait WriteResult

  object WriteResult {

    /**
     * The result of `write` when the channel has become invalid
     *
     * This can happen if some other process (e.g. a concurrent loader) has altered the Snowflake
     * table
     */
    case object ChannelIsInvalid extends WriteResult

    /**
     * The result of `write` when the channel is valid
     *
     * @param value
     *   Contains details of any failures to write events to Snowflake. If the write was completely
     *   successful then this list is empty.
     */
    case class WriteFailures(value: List[ChannelProvider.WriteFailure]) extends WriteResult

  }

  /** A large number so we don't limit the number of permits for calls to `flush` and `enqueue` */
  private val allAvailablePermits: Long = Long.MaxValue

  def make[F[_]: Async](
    config: Config.Snowflake,
    snowflakeHealth: SnowflakeHealth[F],
    batchingConfig: Config.Batching,
    retriesConfig: Config.Retries
  ): Resource[F, ChannelProvider[F]] =
    for {
      client <- createClient(config, batchingConfig)
      channelResource = createChannel(config, client, snowflakeHealth, retriesConfig)
      (hs, channel) <- Hotswap.apply(channelResource)
      ref <- Resource.eval(Ref[F].of(channel))
      sem <- Resource.eval(Semaphore[F](allAvailablePermits))
    } yield impl(ref, hs, sem, channelResource)

  private def impl[F[_]: Async](
    ref: Ref[F, SnowflakeStreamingIngestChannel],
    hs: Hotswap[F, SnowflakeStreamingIngestChannel],
    sem: Semaphore[F],
    next: Resource[F, SnowflakeStreamingIngestChannel]
  ): ChannelProvider[F] =
    new ChannelProvider[F] {
      def reset: F[Unit] =
        withAllPermits(sem) { // Must have **all** permits so we don't conflict with a write
          ref.get.flatMap { channel =>
            if (channel.isValid())
              // We might have concurrent fibers calling `reset`.  This just means another fiber
              // has already reset this channel.
              Sync[F].unit
            else
              Sync[F].uncancelable { _ =>
                for {
                  _ <- hs.clear
                  channel <- hs.swap(next)
                  _ <- ref.set(channel)
                } yield ()
              }
          }
        }

      def withClosedChannel[A](fa: F[A]): F[A] =
        withAllPermits(sem) { // Must have **all** permites so we don't conflict with a write
          Sync[F].uncancelable { _ =>
            for {
              _ <- hs.clear
              a <- fa
              channel <- hs.swap(next)
              _ <- ref.set(channel)
            } yield a
          }
        }

      def write(rows: Iterable[Map[String, AnyRef]]): F[WriteResult] =
        sem.permit
          .use[WriteResult] { _ =>
            for {
              channel <- ref.get
              response <- Sync[F].blocking(channel.insertRows(rows.map(_.asJava).asJava, null))
              _ <- flushChannel[F](channel)
              isValid <- Sync[F].delay(channel.isValid)
            } yield if (isValid) WriteResult.WriteFailures(parseResponse(response)) else WriteResult.ChannelIsInvalid
          }
          .recover {
            case sfe: SFException if sfe.getVendorCode === SFErrorCode.INVALID_CHANNEL.getMessageCode =>
              WriteResult.ChannelIsInvalid
          }
    }

  /** Wraps a `F[A]` so it only runs when no other fiber is using the channel at the same time */
  private def withAllPermits[F[_]: Sync, A](sem: Semaphore[F])(f: F[A]): F[A] =
    Sync[F].uncancelable { poll =>
      for {
        _ <- poll(sem.acquireN(allAvailablePermits))
        a <- f.guarantee(sem.releaseN(allAvailablePermits))
      } yield a
    }

  private def parseResponse(response: InsertValidationResponse): List[WriteFailure] =
    response.getInsertErrors.asScala.map { insertError =>
      WriteFailure(
        insertError.getRowIndex,
        Option(insertError.getExtraColNames).fold(List.empty[String])(_.asScala.toList),
        insertError.getException
      )
    }.toList

  private def createChannel[F[_]: Async](
    config: Config.Snowflake,
    client: SnowflakeStreamingIngestClient,
    snowflakeHealth: SnowflakeHealth[F],
    retriesConfig: Config.Retries
  ): Resource[F, SnowflakeStreamingIngestChannel] = {
    val request = OpenChannelRequest
      .builder(config.channel)
      .setDBName(config.database)
      .setSchemaName(config.schema)
      .setTableName(config.table)
      .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
      .setDefaultTimezone(ZoneOffset.UTC)
      .build

    val make = Logger[F].info(s"Opening channel ${config.channel}") *>
      SnowflakeRetrying.retryIndefinitely(snowflakeHealth, retriesConfig) {
        Async[F].blocking(client.openChannel(request))
      }

    Resource.make(make) { channel =>
      Logger[F].info(s"Closing channel ${config.channel}") *>
        Async[F]
          .fromCompletableFuture {
            Async[F].delay {
              channel.close()
            }
          }
          .void
          .recover {
            case sfe: SFException if sfe.getVendorCode === SFErrorCode.INVALID_CHANNEL.getMessageCode =>
              // We have already handled errors associated with invalid channel
              ()
          }
    }
  }

  private def channelProperties(config: Config.Snowflake, batchingConfig: Config.Batching): Properties = {
    val props = new Properties()
    props.setProperty("user", config.user)
    props.setProperty("private_key", config.privateKey)
    config.privateKeyPassphrase.foreach(props.setProperty("private_key_passphrase", _))
    config.role.foreach(props.setProperty("role", _))
    props.setProperty("url", config.url.getFullUrl)
    props.setProperty(ParameterProvider.ENABLE_SNOWPIPE_STREAMING_METRICS, "false")

    // Disable SDK's background flushing because we manage it ourselves
    props.setProperty(ParameterProvider.BUFFER_FLUSH_INTERVAL_IN_MILLIS, Long.MaxValue.toString)
    props.setProperty(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, Long.MaxValue.toString)
    props.setProperty(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, "0")
    props.setProperty(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, "0")
    props.setProperty(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, "0")
    props.setProperty(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, Long.MaxValue.toString)
    props.setProperty(ParameterProvider.IO_TIME_CPU_RATIO, batchingConfig.uploadConcurrency.toString)

    props
  }

  private def createClient[F[_]: Sync](
    config: Config.Snowflake,
    batchingConfig: Config.Batching
  ): Resource[F, SnowflakeStreamingIngestClient] = {
    val make = Sync[F].delay {
      SnowflakeStreamingIngestClientFactory
        .builder("snowplow") // client name is not important
        .setProperties(channelProperties(config, batchingConfig))
        // .setParameterOverrides(Map.empty.asJava) // Not needed, as all params can also be set with Properties
        .build
    }
    Resource.fromAutoCloseable(make)
  }

  /**
   * Flushes the channel
   *
   * The public interface of the Snowflake SDK does not tell us when the events are safely written
   * to Snowflake. So we must cast it to an Internal class so we get access to the `flush()` method.
   */
  private def flushChannel[F[_]: Async](channel: SnowflakeStreamingIngestChannel): F[Unit] =
    Async[F].fromCompletableFuture {
      Async[F].delay(SnowsFlakePlowInterop.flushChannel(channel))
    }.void

}
