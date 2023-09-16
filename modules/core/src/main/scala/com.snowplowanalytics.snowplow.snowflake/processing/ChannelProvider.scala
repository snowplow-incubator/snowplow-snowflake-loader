/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.implicits._
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
import net.snowflake.ingest.utils.{ParameterProvider, SFException}

import com.snowplowanalytics.snowplow.snowflake.Config

import java.time.ZoneOffset
import java.util.Properties
import scala.jdk.CollectionConverters._

trait ChannelProvider[F[_]] {

  /**
   * Closes the open channel and opens a new channel
   *
   * This should be called after altering the table to add new columns. The newly opened channel
   * will be able to use the new columns.
   */
  def reset: F[Unit]

  /**
   * Insert rows into Snowflake
   *
   * @param rows
   *   The rows to be inserted
   * @return
   *   List of the details of any insert failures. Empty list implies complete success.
   */
  def insert(rows: Seq[Map[String, AnyRef]]): F[List[ChannelProvider.InsertFailure]]
}

object ChannelProvider {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  /**
   * The result of trying to insert an event
   * @param index
   *   Refers to the row number in the batch of attempted events
   * @param extraCols
   *   The column names which were present in the batch but missing in the table
   * @param cause
   *   The Snowflake exception, whose error code and message describes the reason for the failed
   *   insert
   */
  case class InsertFailure(
    index: Long,
    extraCols: List[String],
    cause: SFException
  )

  def make[F[_]: Async](config: Config.Snowflake): Resource[F, ChannelProvider[F]] =
    for {
      client <- createClient(config)
      hs <- Hotswap.create[F, SnowflakeStreamingIngestChannel]
      channel <- Resource.eval(hs.swap(createChannel(config, client)))
      ref <- Resource.eval(Ref[F].of(channel))
      sem <- Resource.eval(Semaphore[F](1))
    } yield impl(ref, hs, sem, createChannel(config, client))

  private def impl[F[_]: Sync](
    ref: Ref[F, SnowflakeStreamingIngestChannel],
    hs: Hotswap[F, SnowflakeStreamingIngestChannel],
    sem: Semaphore[F],
    next: Resource[F, SnowflakeStreamingIngestChannel]
  ): ChannelProvider[F] =
    new ChannelProvider[F] {
      def reset: F[Unit] =
        sem.permit.use { _ =>
          Sync[F].uncancelable { _ =>
            for {
              _ <- hs.clear
              channel <- hs.swap(next)
              _ <- ref.set(channel)
            } yield ()
          }
        }

      def insert(rows: Seq[Map[String, AnyRef]]): F[List[InsertFailure]] =
        sem.permit.use { _ =>
          for {
            channel <- ref.get
            response <- Sync[F].blocking(channel.insertRows(rows.map(_.asJava).asJava, null))
          } yield parseResponse(response)
        }
    }

  private def parseResponse(response: InsertValidationResponse): List[InsertFailure] =
    response.getInsertErrors.asScala.map { insertError =>
      InsertFailure(
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
    props
  }

  private def createClient[F[_]: Sync](config: Config.Snowflake): Resource[F, SnowflakeStreamingIngestClient] = {
    val make = Sync[F].delay {
      SnowflakeStreamingIngestClientFactory
        .builder("snowplow") // client name is not important
        .setProperties(channelProperties(config))
        // .setParameterOverrides(Map.empty.asJava) // TODO: set param overrides
        .build
    }
    Resource.fromAutoCloseable(make)
  }

}
