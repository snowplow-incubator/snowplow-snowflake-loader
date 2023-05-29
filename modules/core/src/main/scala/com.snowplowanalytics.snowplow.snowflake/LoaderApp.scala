/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.effect.{ExitCode, IO, Resource}
import io.circe.Decoder
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.Opts

import scala.concurrent.duration.DurationInt

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.loaders.AppInfo

abstract class LoaderApp[SourceConfig: Decoder, SinkConfig: Decoder](
  info: AppInfo
) extends CommandIOApp(name = LoaderApp.helpCommand(info), header = info.dockerAlias, version = info.version) {

  override def runtimeConfig =
    super.runtimeConfig.copy(cpuStarvationCheckInterval = 10.seconds)

  type SinkProvider = SinkConfig => Resource[IO, Sink[IO]]
  type SourceProvider = SourceConfig => SourceAndAck[IO]

  def source: SourceProvider
  def badSink: SinkProvider

  final def main: Opts[IO[ExitCode]] = Run.fromCli(info, source, badSink)

}

object LoaderApp {

  private def helpCommand(appInfo: AppInfo) = s"docker run ${appInfo.dockerAlias}"

}
