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

import cats.effect.{ExitCode, IO, Resource}
import cats.effect.metrics.CpuStarvationWarningMetrics
import io.circe.Decoder
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.runtime.AppInfo

abstract class LoaderApp[SourceConfig: Decoder, SinkConfig: Decoder](
  info: AppInfo
) extends CommandIOApp(name = LoaderApp.helpCommand(info), header = info.dockerAlias, version = info.version) {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def onCpuStarvationWarn(metrics: CpuStarvationWarningMetrics): IO[Unit] =
    Logger[IO].debug(s"Cats Effect measured responsiveness in excess of ${metrics.starvationInterval * metrics.starvationThreshold}")

  type SinkProvider   = SinkConfig => Resource[IO, Sink[IO]]
  type SourceProvider = SourceConfig => IO[SourceAndAck[IO]]

  def source: SourceProvider
  def badSink: SinkProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(info, source, badSink)

}

object LoaderApp {

  private def helpCommand(appInfo: AppInfo) = s"docker run ${appInfo.dockerAlias}"

}
