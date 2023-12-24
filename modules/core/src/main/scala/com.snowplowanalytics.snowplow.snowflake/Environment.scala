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

import cats.effect.unsafe.implicits.global
import cats.effect.{Async, Resource}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AppInfo, HealthProbe}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.{Channel, Coldswap, SnowflakeHealth, TableManager}
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tableManager: TableManager[F],
  channel: Coldswap[F, Channel[F]],
  metrics: Metrics[F],
  batching: Config.Batching,
  schemasToSkip: List[SchemaCriterion]
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
      channelResource <- Channel.make(config.output.good, snowflakeHealth, config.batching, config.retries, monitoring)
      channelColdswap <- Coldswap.make(channelResource)
    } yield Environment(
      appInfo       = appInfo,
      source        = sourceAndAck,
      badSink       = badSink,
      httpClient    = httpClient,
      tableManager  = tableManager,
      channel       = channelColdswap,
      metrics       = metrics,
      batching      = config.batching,
      schemasToSkip = config.skipSchemas
    )
}
