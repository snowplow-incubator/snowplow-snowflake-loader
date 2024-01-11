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
import com.snowplowanalytics.snowplow.snowflake.AppHealth.Service
import com.snowplowanalytics.snowplow.snowflake.processing.{Channel, TableManager}
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tableManager: TableManager[F],
  channel: Channel.Provider[F],
  metrics: Metrics[F],
  appHealth: AppHealth[F],
  batching: Config.Batching,
  schemasToSkip: List[SchemaCriterion],
  badRowMaxSize: Int
)

object Environment {

  private val initialAppHealth: Map[Service, Boolean] = Map(
    Service.Snowflake -> false,
    Service.BadSink -> true
  )

  def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    config: Config[SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toSource: SourceConfig => F[SourceAndAck[F]],
    toSink: SinkConfig => Resource[F, Sink[F]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- Sentry.capturingAnyException(appInfo, config.monitoring.sentry)
      sourceAndAck <- Resource.eval(toSource(config.input))
      appHealth <- Resource.eval(AppHealth.init(config.monitoring.healthProbe.unhealthyLatency, sourceAndAck, initialAppHealth))
      _ <- HealthProbe.resource(
             config.monitoring.healthProbe.port,
             appHealth.status()
           )
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      monitoring <- Monitoring.create[F](config.monitoring.webhook, appInfo, httpClient)
      badSink <- toSink(config.output.bad.sink)
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      tableManager <- Resource.eval(TableManager.make(config.output.good, appHealth, config.retries, monitoring))
      channelOpener <- Channel.opener(config.output.good, config.batching, config.retries, monitoring, appHealth)
      channelProvider <- Channel.provider(channelOpener, config.retries, appHealth, monitoring)
    } yield Environment(
      appInfo       = appInfo,
      source        = sourceAndAck,
      badSink       = badSink,
      httpClient    = httpClient,
      tableManager  = tableManager,
      channel       = channelProvider,
      metrics       = metrics,
      appHealth     = appHealth,
      batching      = config.batching,
      schemasToSkip = config.skipSchemas,
      badRowMaxSize = config.output.bad.maxRecordSize
    )
}
