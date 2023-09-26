/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.Id
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import net.snowflake.ingest.utils.SnowflakeURL

import scala.concurrent.duration.FiniteDuration
import scala.util.Try

import com.snowplowanalytics.snowplow.loaders.{Metrics => CommonMetrics, Telemetry}

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  batching: Config.Batching,
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring
)

object Config {

  case class Output[+Sink](good: Snowflake, bad: Sink)

  case class Snowflake(
    url: SnowflakeURL,
    user: String,
    privateKey: String,
    privateKeyPassphrase: Option[String],
    role: Option[String],
    database: String,
    schema: String,
    table: String,
    channel: String,
    jdbcLoginTimeout: FiniteDuration,
    jdbcNetworkTimeout: FiniteDuration,
    jdbcQueryTimeout: FiniteDuration
  )

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration,
    uploadConcurrency: Int
  )

  case class Metrics(
    statsd: Option[CommonMetrics.StatsdConfig]
  )

  private case class StatsdUnresolved(
    hostname: Option[String],
    port: Int,
    tags: Map[String, String],
    period: FiniteDuration,
    prefix: String
  )

  private object Statsd {

    def resolve(statsd: StatsdUnresolved): Option[CommonMetrics.StatsdConfig] =
      statsd match {
        case StatsdUnresolved(Some(hostname), port, tags, period, prefix) =>
          Some(CommonMetrics.StatsdConfig(hostname, port, tags, period, prefix))
        case StatsdUnresolved(None, _, _, _, _) =>
          None
      }
  }

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry]
  )

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val urlDecoder = Decoder.decodeString.emapTry { str =>
      Try(new SnowflakeURL(str))
    }
    implicit val snowflake     = deriveConfiguredDecoder[Snowflake]
    implicit val output        = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching      = deriveConfiguredDecoder[Batching]
    implicit val telemetry     = deriveConfiguredDecoder[Telemetry.Config]
    implicit val statsdDecoder = deriveConfiguredDecoder[StatsdUnresolved].map(Statsd.resolve(_))
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val metricsDecoder    = deriveConfiguredDecoder[Metrics]
    implicit val monitoringDecoder = deriveConfiguredDecoder[Monitoring]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
