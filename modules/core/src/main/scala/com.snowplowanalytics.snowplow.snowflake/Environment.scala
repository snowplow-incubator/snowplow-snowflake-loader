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

import cats.implicits._
import cats.effect.unsafe.implicits.global
import cats.effect.{Async, Resource}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, Webhook}
import com.snowplowanalytics.snowplow.sinks.Sink
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
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
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
      sourceAndAck <- Resource.eval(toSource(config.input))
      sourceReporter = sourceAndAck.isHealthy(config.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      _ <- HealthProbe.resource(config.monitoring.healthProbe.port, appHealth)
      _ <- Webhook.resource(config.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <- toSink(config.output.bad.sink).onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      tableManager <- Resource.eval(TableManager.make(config.output.good, appHealth, config.retries))
      channelOpener <- Channel.opener(config.output.good, config.batching, config.retries, appHealth)
      channelProvider <- Channel.provider(channelOpener, config.retries, appHealth)
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
