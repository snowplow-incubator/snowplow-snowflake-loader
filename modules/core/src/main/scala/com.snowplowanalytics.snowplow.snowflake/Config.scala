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

import cats.Id
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
import com.snowplowanalytics.snowplow.runtime.{HttpClient, Metrics => CommonMetrics, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.snowplow.runtime.HealthProbe.decoders._

case class Config[+Source, +Sink](
  input: Source,
  output: Config.Output[Sink],
  batching: Config.Batching,
  cpuParallelismFactor: BigDecimal,
  retries: Config.Retries,
  skipSchemas: List[SchemaCriterion],
  telemetry: Telemetry.Config,
  monitoring: Config.Monitoring,
  http: Config.Http
)

object Config {

  case class Output[+Sink](
    good: Snowflake,
    bad: SinkWithMaxSize[Sink]
  )

  case class MaxRecordSize(maxRecordSize: Int)

  case class SinkWithMaxSize[+Sink](sink: Sink, maxRecordSize: Int)

  case class Snowflake(
    url: Snowflake.Url,
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

  object Snowflake {
    case class Url(full: String, jdbc: String)
  }

  case class Batching(
    maxBytes: Long,
    maxDelay: FiniteDuration,
    uploadParallelismFactor: BigDecimal
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
    webhook: Webhook.Config
  )

  case class SetupErrorRetries(delay: FiniteDuration)
  case class TransientErrorRetries(delay: FiniteDuration, attempts: Int)

  case class Retries(
    setupErrors: Retrying.Config.ForSetup,
    transientErrors: Retrying.Config.ForTransient
  )

  case class Http(client: HttpClient.Config)

  implicit def decoder[Source: Decoder, Sink: Decoder]: Decoder[Config[Source, Sink]] = {
    implicit val configuration = Configuration.default.withDiscriminator("type")
    implicit val urlDecoder = Decoder.decodeString.emapTry { str =>
      Try {
        val sdkUrl = new SnowflakeURL(str)
        Snowflake.Url(sdkUrl.getFullUrl, sdkUrl.getJdbcUrl)
      }
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
    implicit val metricsDecoder     = deriveConfiguredDecoder[Metrics]
    implicit val healthProbeDecoder = deriveConfiguredDecoder[HealthProbe]
    implicit val monitoringDecoder  = deriveConfiguredDecoder[Monitoring]
    implicit val retriesDecoder     = deriveConfiguredDecoder[Retries]
    implicit val httpDecoder        = deriveConfiguredDecoder[Http]
    deriveConfiguredDecoder[Config[Source, Sink]]
  }

}
