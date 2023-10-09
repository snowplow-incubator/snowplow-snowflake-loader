/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
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
import net.snowflake.ingest.utils.{ParameterProvider, SFException}

import com.snowplowanalytics.snowplow.snowflake.Config

import java.time.ZoneOffset
import java.util.Properties
import scala.jdk.CollectionConverters._

trait ChannelProvider[F[_]] {

  /**
   * Wraps an action which requires the channel to be closed
   *
   * This should be called when altering the table to add new columns. The newly opened channel will
   * be able to use the new columns.
   */
  def withClosedChannel[A](fa: F[A]): F[A]

  /**
   * Enqueues rows to be sent to Snowflake
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def enqueue(rows: Seq[Map[String, AnyRef]]): F[List[ChannelProvider.EnqueueFailure]]

  /** Flush enqueued events */
  def flush: F[Unit]
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
  case class EnqueueFailure(
    index: Long,
    extraCols: List[String],
    cause: SFException
  )

  /** A large number so we don't limit the number of permits for calls to `flush` and `enqueue` */
  private val allAvailablePermits: Long = Long.MaxValue

  def make[F[_]: Async](config: Config.Snowflake): Resource[F, ChannelProvider[F]] =
    for {
      client <- createClient(config)
      hs <- Hotswap.create[F, SnowflakeStreamingIngestChannel]
      channel <- Resource.eval(hs.swap(createChannel(config, client)))
      ref <- Resource.eval(Ref[F].of(channel))
      sem <- Resource.eval(Semaphore[F](allAvailablePermits))
    } yield impl(ref, hs, sem, createChannel(config, client))

  private def impl[F[_]: Async](
    ref: Ref[F, SnowflakeStreamingIngestChannel],
    hs: Hotswap[F, SnowflakeStreamingIngestChannel],
    sem: Semaphore[F],
    next: Resource[F, SnowflakeStreamingIngestChannel]
  ): ChannelProvider[F] =
    new ChannelProvider[F] {
      def withClosedChannel[A](fa: F[A]): F[A] =
        withAllPermits(sem) {
          Sync[F].uncancelable { _ =>
            for {
              _ <- hs.clear
              a <- fa
              channel <- hs.swap(next)
              _ <- ref.set(channel)
            } yield a
          }
        }

      def enqueue(rows: Seq[Map[String, AnyRef]]): F[List[EnqueueFailure]] =
        sem.permit.use { _ =>
          for {
            channel <- ref.get
            response <- Sync[F].blocking(channel.insertRows(rows.map(_.asJava).asJava, null))
          } yield parseResponse(response)
        }

      def flush: F[Unit] =
        sem.permit.use { _ =>
          for {
            channel <- ref.get
            _ <- flushChannel[F](channel)
          } yield ()
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

  private def parseResponse(response: InsertValidationResponse): List[EnqueueFailure] =
    response.getInsertErrors.asScala.map { insertError =>
      EnqueueFailure(
        insertError.getRowIndex,
        Option(insertError.getExtraColNames).fold(List.empty[String])(_.asScala.toList),
        insertError.getException
      )
    }.toList

  private def createChannel[F[_]: Async](
    config: Config.Snowflake,
    client: SnowflakeStreamingIngestClient
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
      Async[F].blocking(client.openChannel(request))

    Resource.make(make) { channel =>
      Logger[F].info(s"Closing channel ${config.channel}") *>
        Async[F].fromCompletableFuture {
          Async[F].delay {
            channel.close()
          }
        }.void
    }
  }

  private def channelProperties(config: Config.Snowflake): Properties = {
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

    props
  }

  private def createClient[F[_]: Sync](config: Config.Snowflake): Resource[F, SnowflakeStreamingIngestClient] = {
    val make = Sync[F].delay {
      SnowflakeStreamingIngestClientFactory
        .builder("snowplow") // client name is not important
        .setProperties(channelProperties(config))
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
