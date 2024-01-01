/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, Resource}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AppInfo, HealthProbe}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.{ChannelProvider, SnowflakeHealth, TableManager}
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tableManager: TableManager[F],
  channelProvider: ChannelProvider[F],
  metrics: Metrics[F],
  batching: Config.Batching,
  schemasToSkip: List[SchemaCriterion],
  badRowMaxSize: Int
)

object Environment {

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config[SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- Sentry.capturingAnyException(appInfo, config.monitoring.sentry)
      snowflakeHealth <- Resource.eval(SnowflakeHealth.initUnhealthy[F])
      sourceAndAck <- Resource.eval(toSource(config.input))
      _ <- HealthProbe.resource(
             config.monitoring.healthProbe.port,
             AppHealth.isHealthy(config.monitoring.healthProbe, sourceAndAck, snowflakeHealth)
           )
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      monitoring <- Monitoring.create[F](config.monitoring.webhook, appInfo, httpClient)
      badSink <- toSink(config.output.bad)
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      tableManager <- Resource.eval(TableManager.make(config.output.good, snowflakeHealth, config.retries, monitoring))
      _ <- Resource.eval(tableManager.initializeEventsTable())
      channelProvider <- ChannelProvider.make(config.output.good, snowflakeHealth, config.batching, config.retries, monitoring)
    } yield Environment(
      appInfo         = appInfo,
      source          = sourceAndAck,
      badSink         = badSink,
      httpClient      = httpClient,
      tableManager    = tableManager,
      channelProvider = channelProvider,
      metrics         = metrics,
      batching        = config.batching,
      schemasToSkip   = config.skipSchemas,
      badRowMaxSize   = config.output.badRowMaxSize
    )
}
