/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import cats.effect.{Async, Resource}
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.{AppHealth, AppInfo, HealthProbe, HttpClient, Sentry, Webhook}
import com.snowplowanalytics.snowplow.streams.{Factory, Sink, SourceAndAck}
import com.snowplowanalytics.snowplow.streams.compression.DecompressionConfig
import com.snowplowanalytics.snowplow.snowflake.processing.{Channel, TableManager}
import org.http4s.client.Client

/**
 * Resources and runtime-derived configuration needed for processing events
 *
 * @param cpuParallelism
 *   The processing Pipe involves several steps, some of which are cpu-intensive. We run
 *   cpu-intensive steps in parallel, so that on big instances we can take advantage of all cores.
 *   For each of those cpu-intensive steps, `cpuParallelism` controls the parallelism of that step.
 *
 * Other params are self-explanatory
 */
case class Environment[F[_]](
  appInfo: AppInfo,
  source: SourceAndAck[F],
  badSink: Sink[F],
  httpClient: Client[F],
  tableManager: TableManager[F],
  channels: Vector[Channel.Provider[F]],
  metrics: Metrics[F],
  appHealth: AppHealth.Interface[F, Alert, RuntimeService],
  batching: Config.Batching,
  cpuParallelism: Int,
  schemasToSkip: List[SchemaCriterion],
  badRowMaxSize: Int,
  decompression: DecompressionConfig
)

object Environment {

  def fromConfig[F[_]: Async, FactoryConfig, SourceConfig, SinkConfig](
    config: Config[FactoryConfig, SourceConfig, SinkConfig],
    appInfo: AppInfo,
    toFactory: FactoryConfig => Resource[F, Factory[F, SourceConfig, SinkConfig]]
  ): Resource[F, Environment[F]] =
    for {
      _ <- Sentry.enable[F](appInfo, config.monitoring.sentry)
      factory <- toFactory(config.streams)
      sourceAndAck <- factory.source(config.input)
      sourceReporter = sourceAndAck.isHealthy(config.monitoring.healthProbe.unhealthyLatency).map(_.showIfUnhealthy)
      appHealth <- Resource.eval(AppHealth.init[F, Alert, RuntimeService](List(sourceReporter)))
      httpClient <- HttpClient.resource[F](config.http.client)
      metrics <- Metrics.build(config.monitoring.metrics, sourceAndAck)
      _ <- HealthProbe.resource(config.monitoring.healthProbe.port, appHealth, metrics.scrape)
      _ <- Webhook.resource(config.monitoring.webhook, appInfo, httpClient, appHealth)
      badSink <- factory
                   .sink(config.output.bad.sink)
                   .onError(_ => Resource.eval(appHealth.beUnhealthyForRuntimeService(RuntimeService.BadSink)))
      tableManager <- Resource.eval(TableManager.make(config.output.good, appHealth, config.retries))
      cpuParallelism    = chooseCpuParallelism(config)
      uploadParallelism = chooseUploadParallelism(config)
      channelProviders <- Vector.range(0, uploadParallelism).traverse { index =>
                            for {
                              channelOpener <- Channel.opener(config.output.good, config.retries, appHealth, index)
                              channelProvider <- Channel.provider(channelOpener, config.retries, appHealth)
                            } yield channelProvider
                          }
    } yield Environment(
      appInfo        = appInfo,
      source         = sourceAndAck,
      badSink        = badSink,
      httpClient     = httpClient,
      tableManager   = tableManager,
      channels       = channelProviders,
      metrics        = metrics,
      appHealth      = appHealth,
      batching       = config.batching,
      cpuParallelism = cpuParallelism,
      schemasToSkip  = config.skipSchemas,
      badRowMaxSize  = config.output.bad.maxRecordSize,
      decompression  = config.decompression
    )

  /**
   * See the description of `cpuParallelism` on the [[Environment]] class
   *
   * For bigger instances (more cores) we want more parallelism, so that cpu-intensive steps can
   * take advantage of all the cores.
   */
  private def chooseCpuParallelism(config: Config[Any, Any, Any]): Int =
    multiplyByCpuAndRoundUp(config.cpuParallelismFactor)

  /**
   * For bigger instances (more cores) we produce batches more quickly, and so need higher upload
   * parallelism so that uploading does not become bottleneck
   */
  private def chooseUploadParallelism(config: Config[Any, Any, Any]): Int =
    multiplyByCpuAndRoundUp(config.batching.uploadParallelismFactor)

  private def multiplyByCpuAndRoundUp(factor: BigDecimal): Int =
    (Runtime.getRuntime.availableProcessors * factor)
      .setScale(0, BigDecimal.RoundingMode.UP)
      .toInt
}
