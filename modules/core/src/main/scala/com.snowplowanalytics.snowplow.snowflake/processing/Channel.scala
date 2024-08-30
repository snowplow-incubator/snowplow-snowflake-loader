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

import cats.effect.{Async, Poll, Resource, Sync}
import cats.implicits._
import com.snowplowanalytics.snowplow.runtime.AppHealth
import com.snowplowanalytics.snowplow.runtime.processing.Coldswap
import com.snowplowanalytics.snowplow.snowflake.{Alert, Config, RuntimeService}
import net.snowflake.ingest.streaming.internal.SnowsFlakePlowInterop
import net.snowflake.ingest.streaming._
import net.snowflake.ingest.utils.{ErrorCode => SFErrorCode, ParameterProvider, SFException}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.ZoneOffset
import java.util.Properties
import scala.jdk.CollectionConverters._

trait Channel[F[_]] {

  /**
   * Writes rows to Snowflake
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def write(rows: Iterable[Map[String, AnyRef]]): F[Channel.WriteResult]
}

object Channel {

  trait CloseableChannel[F[_]] extends Channel[F] {

    /** Closes the Snowflake channel */
    def close: F[Unit]
  }

  /**
   * Provider of open Snowflake channels
   *
   * The Provider is not responsible for closing any opened channel. Channel lifecycle must be
   * managed by the surrounding application code.
   */
  trait Opener[F[_]] {
    def open: F[CloseableChannel[F]]
  }

  type Provider[F[_]] = Coldswap[F, Channel[F]]

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
    case class WriteFailures(value: List[Channel.WriteFailure]) extends WriteResult

  }

  def opener[F[_]: Async](
    config: Config.Snowflake,
    batchingConfig: Config.Batching,
    retriesConfig: Config.Retries,
    appHealth: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, Opener[F]] =
    for {
      client <- createClient(config, batchingConfig, retriesConfig, appHealth)
    } yield new Opener[F] {
      def open: F[CloseableChannel[F]] = createChannel[F](config, client).map(impl[F])
    }

  def provider[F[_]: Async](
    opener: Opener[F],
    retries: Config.Retries,
    health: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, Provider[F]] =
    Coldswap.make(openerToResource(opener, retries, health))

  private def openerToResource[F[_]: Async](
    opener: Opener[F],
    retries: Config.Retries,
    health: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, Channel[F]] = {

    def make(poll: Poll[F]) = poll {
      SnowflakeRetrying.withRetries(health, retries, Alert.FailedToOpenSnowflakeChannel(_)) {
        opener.open <* health.beHealthyForSetup
      }
    }

    Resource.makeFull(make)(_.close)
  }

  private def impl[F[_]: Async](channel: SnowflakeStreamingIngestChannel): CloseableChannel[F] =
    new CloseableChannel[F] {

      def write(rows: Iterable[Map[String, AnyRef]]): F[WriteResult] = {
        val attempt: F[WriteResult] = for {
          response <- Sync[F].blocking(channel.insertRows(rows.map(_.asJava).asJava, null))
          _ <- flushChannel[F](channel)
          isValid <- Sync[F].delay(channel.isValid)
        } yield if (isValid) WriteResult.WriteFailures(parseResponse(response)) else WriteResult.ChannelIsInvalid

        attempt.recover {
          case sfe: SFException if sfe.getVendorCode === SFErrorCode.INVALID_CHANNEL.getMessageCode =>
            WriteResult.ChannelIsInvalid
        }
      }

      def close: F[Unit] =
        Logger[F].info(s"Closing channel ${channel.getFullyQualifiedName()}") *>
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
    client: SnowflakeStreamingIngestClient
  ): F[SnowflakeStreamingIngestChannel] = {
    val request = OpenChannelRequest
      .builder(config.channel)
      .setDBName(config.database)
      .setSchemaName(config.schema)
      .setTableName(config.table)
      .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
      .setDefaultTimezone(ZoneOffset.UTC)
      .build

    Logger[F].info(s"Opening channel ${config.channel}") *>
      Async[F].blocking(client.openChannel(request)) <*
      Logger[F].info(s"Successfully opened channel ${config.channel}")
  }

  private def channelProperties(config: Config.Snowflake, batchingConfig: Config.Batching): Properties = {
    val props = new Properties()
    props.setProperty("user", config.user)
    props.setProperty("private_key", config.privateKey)
    config.privateKeyPassphrase.foreach(props.setProperty("private_key_passphrase", _))
    config.role.foreach(props.setProperty("role", _))
    props.setProperty("url", config.url.full)
    props.setProperty(ParameterProvider.ENABLE_SNOWPIPE_STREAMING_METRICS, "false")

    // Disable SDK's background flushing because we manage it ourselves
    props.setProperty(ParameterProvider.BUFFER_FLUSH_CHECK_INTERVAL_IN_MILLIS, Long.MaxValue.toString)
    // Max allowed value for MAX_CLIENT_LAG is 10 min. However, background flushing is disabled with above
    // line. Therefore, it isn't very important what value we use in here.
    props.setProperty(ParameterProvider.MAX_CLIENT_LAG, "600000")
    props.setProperty(ParameterProvider.INSERT_THROTTLE_INTERVAL_IN_MILLIS, "0")
    props.setProperty(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, "0")
    props.setProperty(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_BYTES, "0")
    props.setProperty(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, Long.MaxValue.toString)
    props.setProperty(ParameterProvider.IO_TIME_CPU_RATIO, batchingConfig.uploadConcurrency.toString)

    props
  }

  private def createClient[F[_]: Async](
    config: Config.Snowflake,
    batchingConfig: Config.Batching,
    retriesConfig: Config.Retries,
    appHealth: AppHealth.Interface[F, Alert, RuntimeService]
  ): Resource[F, SnowflakeStreamingIngestClient] = {
    def make(poll: Poll[F]) = poll {
      SnowflakeRetrying.withRetries(appHealth, retriesConfig, Alert.FailedToConnectToSnowflake(_)) {
        Logger[F].info(show"Initializing a connection to ${config.url.full}") *>
          Sync[F].blocking {
            SnowflakeStreamingIngestClientFactory
              .builder("Snowplow_Streaming")
              .setProperties(channelProperties(config, batchingConfig))
              // .setParameterOverrides(Map.empty.asJava) // Not needed, as all params can also be set with Properties
              .build
          } <*
          Logger[F].info(show"Successfully initialized connection to ${config.url.full}")
      }
    }
    Resource.makeFull(make)(client => Sync[F].blocking(client.close()))
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
