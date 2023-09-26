/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import cats.effect.{Async, Resource, Sync}
import cats.effect.unsafe.implicits.global
import org.http4s.client.Client
import org.http4s.blaze.client.BlazeClientBuilder
import io.sentry.Sentry

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.{ChannelProvider, TableManager}
import com.snowplowanalytics.snowplow.loaders.AppInfo

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tblManager: TableManager[F],
  channelProvider: ChannelProvider[F],
  metrics: Metrics[F],
  batching: Config.Batching
)

object Environment {

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config[SourceConfig, SinkConfig],
    appInfo: AppInfo,
    source: SourceConfig => SourceAndAck[F],
    sink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- enableSentry[F](appInfo, config.monitoring.sentry)
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      badSink <- sink(config.output.bad)
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      xa <- Resource.eval(SQLUtils.transactor[F](config.output.good))
      _ <- Resource.eval(SQLUtils.createTable(config.output.good, xa))
      tblManager = TableManager.fromTransactor(config.output.good, xa)
      channelProvider <- ChannelProvider.make(config.output.good)
    } yield Environment(
      appInfo         = appInfo,
      source          = source(config.input),
      badSink         = badSink,
      httpClient      = httpClient,
      tblManager      = tblManager,
      channelProvider = channelProvider,
      metrics         = metrics,
      batching        = config.batching
    )

  private def enableSentry[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(c) =>
        val acquire = Sync[F].delay {
          Sentry.init { options =>
            options.setDsn(c.dsn)
            options.setRelease(appInfo.version)
            c.tags.foreach { case (k, v) =>
              options.setTag(k, v)
            }
          }
        }

        Resource.makeCase(acquire) {
          case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(Sentry.captureException(e)).void
          case _                                 => Sync[F].unit
        }
      case None =>
        Resource.unit[F]
    }

}
