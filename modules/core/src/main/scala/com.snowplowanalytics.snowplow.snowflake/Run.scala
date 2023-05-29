/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import cats.effect.{Async, ExitCode, Resource, Sync}
import cats.data.EitherT
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import io.circe.Decoder
import com.monovore.decline.Opts

import com.snowplowanalytics.snowplow.sources.SourceAndAck
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.snowflake.processing.Processing
import com.snowplowanalytics.snowplow.loaders.{AppInfo, ConfigParser, LogUtils, Telemetry}

import java.nio.file.Path

object Run {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromCli[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]]
  ): Opts[F[ExitCode]] = {
    val configPathOpt = Opts.option[Path]("config", help = "path to config file")
    configPathOpt.map(fromConfigPaths(appInfo, toSource, toBadSink, _))
  }

  private def fromConfigPaths[F[_]: Async, SourceConfig: Decoder, SinkConfig: Decoder](
    appInfo: AppInfo,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    pathToConfig: Path
  ): F[ExitCode] = {

    val eitherT = for {
      config <- ConfigParser.configFromFile[F, Config[SourceConfig, SinkConfig]](pathToConfig)
      _ <- EitherT.right[ExitCode](fromConfig(appInfo, toSource, toBadSink, config))
    } yield ExitCode.Success

    eitherT.merge.handleErrorWith { e =>
      Logger[F].error(e)("Exiting") >>
        LogUtils.prettyLogException(e).as(ExitCode.Error)
    }
  }

  private def fromConfig[F[_]: Async, SourceConfig, SinkConfig](
    appInfo: AppInfo,
    toSource: SourceConfig => SourceAndAck[F],
    toBadSink: SinkConfig => Resource[F, Sink[F]],
    config: Config[SourceConfig, SinkConfig]
  ): F[ExitCode] =
    Environment.fromConfig(config, appInfo, toSource, toBadSink).use { env =>
      Processing
        .stream(env)
        .concurrently(Telemetry.stream(config.telemetry, env.appInfo, env.httpClient))
        .concurrently(env.metrics.report)
        .compile
        .drain
        .as(ExitCode.Success)
    }

}
