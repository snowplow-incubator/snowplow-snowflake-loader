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
import cats.effect.{Async, Resource, Sync}
import cats.implicits._
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AppInfo, HealthProbe}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.{ChannelProvider, SnowflakeHealth, SnowflakeRetrying, TableManager}
import com.snowplowanalytics.snowplow.sources.SourceAndAck
import io.sentry.Sentry
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.client.Client

case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tblManager: TableManager[F],
  channelProvider: ChannelProvider[F],
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
      snowflakeHealth <- Resource.eval(SnowflakeHealth.initUnhealthy[F])
      sourceAndAck <- Resource.eval(toSource(config.input))
      _ <- HealthProbe.resource(
             config.monitoring.healthProbe.port,
             AppHealth.isHealthy(config.monitoring.healthProbe, sourceAndAck, snowflakeHealth)
           )
      _ <- enableSentry[F](appInfo, config.monitoring.sentry)
      httpClient <- BlazeClientBuilder[F].withExecutionContext(global.compute).resource
      badSink <- toSink(config.output.bad)
      metrics <- Resource.eval(Metrics.build(config.monitoring.metrics))
      xa <- Resource.eval(SQLUtils.transactor[F](config.output.good))
      _ <- Resource.eval(SnowflakeRetrying.retryIndefinitely(snowflakeHealth, config.retries)(SQLUtils.createTable(config.output.good, xa)))
      tblManager = TableManager.fromTransactor(config.output.good, xa, snowflakeHealth, config.retries)
      channelProvider <- ChannelProvider.make(config.output.good, snowflakeHealth, config.batching, config.retries)

    } yield Environment(
      appInfo         = appInfo,
      source          = sourceAndAck,
      badSink         = badSink,
      httpClient      = httpClient,
      tblManager      = tblManager,
      channelProvider = channelProvider,
      metrics         = metrics,
      batching        = config.batching,
      schemasToSkip   = config.skipSchemas
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
