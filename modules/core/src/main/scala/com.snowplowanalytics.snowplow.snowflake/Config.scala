/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.Id
import cats.syntax.either._
import io.circe.Decoder
import io.circe.generic.extras.semiauto._
import io.circe.generic.extras.Configuration
import io.circe.config.syntax._
import net.snowflake.ingest.utils.SnowflakeURL
import com.comcast.ip4s.Port
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs.schemaCriterionDecoder

import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics, Telemetry}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._
import org.http4s.{ParseFailure, Uri}

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  batching: Config.Batching,
  retries: Config.Retries,
  skipSchemas: List[SchemaCriterion],
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring
)

object Config {

  case class Output[+Sink](
    good: Snowflake,
    bad: SinkWithMaxSize[Sink]
  )

  case class MaxRecordSize(maxRecordSize: Int)

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

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

  case class SentryM[M[_]](
    dsn: M[String],
    tags: Map[String, String]
  )

  type Sentry = SentryM[Id]

  case class HealthProbe(port: Port, unhealthyLatency: FiniteDuration)

  case class Monitoring(
    metrics: Metrics,
    sentry: Option[Sentry],
    healthProbe: HealthProbe,
    webhook: Option[Webhook]
  )

  final case class Webhook(endpoint: Uri, tags: Map[String, String])

  case class Retries(backoff: FiniteDuration)

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val urlDecoder = Decoder.decodeString.emapTry { str =>
      Try(new SnowflakeURL(str))
    }
    implicit val snowflake = deriveConfiguredDecoder[Snowflake]
    implicit val sinkWithMaxSize = for {
      sink <- Decoder[Sink]
      maxSize <- deriveConfiguredDecoder[MaxRecordSize]
    } yield SinkWithMaxSize(sink, maxSize.maxRecordSize)
    implicit val output   = deriveConfiguredDecoder[Output[Sink]]
    implicit val batching = deriveConfiguredDecoder[Batching]
    implicit val sentryDecoder = deriveConfiguredDecoder[SentryM[Option]]
      .map[Option[Sentry]] {
        case SentryM(Some(dsn), tags) =>
          Some(SentryM[Id](dsn, tags))
        case SentryM(None, _) =>
          None
      }
    implicit val http4sUriDecoder: Decoder[Uri] =
      Decoder[String].emap(s => Either.catchOnly[ParseFailure](Uri.unsafeFromString(s)).leftMap(_.toString))

    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val webhookDecoder     = deriveConfiguredDecoder[Webhook]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
