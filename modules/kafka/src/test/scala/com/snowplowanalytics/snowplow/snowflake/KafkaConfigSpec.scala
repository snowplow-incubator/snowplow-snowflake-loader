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
import cats.effect.testing.specs2.CatsEffect
import cats.effect.{ExitCode, IO}
import com.comcast.ip4s.Port
import com.snowplowanalytics.iglu.core.SchemaCriterion
import com.snowplowanalytics.snowplow.runtime.Metrics.StatsdConfig
import com.snowplowanalytics.snowplow.runtime.{ConfigParser, Telemetry}
import com.snowplowanalytics.snowplow.sinks.kafka.KafkaSinkConfig
import com.snowplowanalytics.snowplow.snowflake.Config.Snowflake
import com.snowplowanalytics.snowplow.sources.kafka.KafkaSourceConfig
import org.http4s.implicits.http4sLiteralsSyntax
import org.specs2.Specification

import java.nio.file.Paths
import scala.concurrent.duration.DurationInt

class KafkaConfigSpec extends Specification with CatsEffect {

  def is = s2"""
   Config parse should be able to parse 
    minimal kafka config $minimal
    extended kafka config $extended
  """

  private def minimal =
    assert(
      resource = "/config.azure.minimal.hocon",
      expectedResult = Right(
        KafkaConfigSpec.minimalConfig
      )
    )

  private def extended =
    assert(
      resource = "/config.azure.reference.hocon",
      expectedResult = Right(
        KafkaConfigSpec.extendedConfig
      )
    )

  private def assert(resource: String, expectedResult: Either[ExitCode, Config[KafkaSourceConfig, KafkaSinkConfig]]) = {
    val path = Paths.get(getClass.getResource(resource).toURI)
    ConfigParser.configFromFile[IO, Config[KafkaSourceConfig, KafkaSinkConfig]](path).value.map { result =>
      result must beEqualTo(expectedResult)
    }
  }
}

object KafkaConfigSpec {
  private val minimalConfig = Config[KafkaSourceConfig, KafkaSinkConfig](
    input = KafkaSourceConfig(
      topicName        = "sp-dev-enriched",
      bootstrapServers = "localhost:9092",
      consumerConf = Map(
        "group.id" -> "snowplow-snowflake-loader",
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "latest",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      )
    ),
    output = Config.Output(
      good = Config.Snowflake(
        url = Snowflake.Url(
          full = "orgname.accountname.snowflakecomputing.com:443",
          jdbc = "jdbc:snowflake://orgname.accountname.snowflakecomputing.com:443"
        ),
        user                 = "snowplow",
        privateKey           = "secretPrivateKey",
        privateKeyPassphrase = None,
        role                 = None,
        database             = "snowplow",
        schema               = "atomic",
        table                = "events",
        channel              = "snowplow",
        jdbcLoginTimeout     = 1.minute,
        jdbcNetworkTimeout   = 1.minute,
        jdbcQueryTimeout     = 1.minute
      ),
      bad = Config.SinkWithMaxSize(
        sink = KafkaSinkConfig(
          topicName        = "sp-dev-bad",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "snowplow-snowflake-loader",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000
      )
    ),
    batching = Config.Batching(
      maxBytes          = 16000000,
      maxDelay          = 1.second,
      uploadConcurrency = 3
    ),
    retries = Config.Retries(
      setupErrors     = Config.SetupErrorRetries(delay = 30.seconds),
      transientErrors = Config.TransientErrorRetries(delay = 1.second, attempts = 5)
    ),
    skipSchemas = List.empty,
    telemetry = Telemetry.Config(
      disable         = false,
      interval        = 15.minutes,
      collectorUri    = "collector-g.snowplowanalytics.com",
      collectorPort   = 443,
      secure          = true,
      userProvidedId  = None,
      autoGeneratedId = None,
      instanceId      = None,
      moduleName      = None,
      moduleVersion   = None
    ),
    monitoring = Config.Monitoring(
      metrics     = Config.Metrics(None),
      sentry      = None,
      healthProbe = Config.HealthProbe(port = Port.fromInt(8000).get, unhealthyLatency = 5.minutes),
      webhook     = None
    )
  )

  /**
   * Environment variables for Snowflake private key and passphrase are set in BuildSettings.scala
   */
  private val extendedConfig = Config[KafkaSourceConfig, KafkaSinkConfig](
    input = KafkaSourceConfig(
      topicName        = "sp-dev-enriched",
      bootstrapServers = "localhost:9092",
      consumerConf = Map(
        "group.id" -> "snowplow-snowflake-loader",
        "enable.auto.commit" -> "false",
        "allow.auto.create.topics" -> "false",
        "auto.offset.reset" -> "earliest",
        "security.protocol" -> "SASL_SSL",
        "sasl.mechanism" -> "OAUTHBEARER",
        "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      )
    ),
    output = Config.Output(
      good = Config.Snowflake(
        url = Snowflake.Url(
          full = "orgname.accountname.snowflakecomputing.com:443",
          jdbc = "jdbc:snowflake://orgname.accountname.snowflakecomputing.com:443"
        ),
        user                 = "snowplow",
        privateKey           = "secretPrivateKey",
        privateKeyPassphrase = Some("secretKeyPassphrase"),
        role                 = Some("snowplow_loader"),
        database             = "snowplow",
        schema               = "atomic",
        table                = "events",
        channel              = "snowplow",
        jdbcLoginTimeout     = 1.minute,
        jdbcNetworkTimeout   = 1.minute,
        jdbcQueryTimeout     = 1.minute
      ),
      bad = Config.SinkWithMaxSize(
        sink = KafkaSinkConfig(
          topicName        = "sp-dev-bad",
          bootstrapServers = "localhost:9092",
          producerConf = Map(
            "client.id" -> "snowplow-snowflake-loader",
            "security.protocol" -> "SASL_SSL",
            "sasl.mechanism" -> "OAUTHBEARER",
            "sasl.jaas.config" -> "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
          )
        ),
        maxRecordSize = 1000000
      )
    ),
    batching = Config.Batching(
      maxBytes          = 16000000,
      maxDelay          = 1.second,
      uploadConcurrency = 1
    ),
    retries = Config.Retries(
      setupErrors     = Config.SetupErrorRetries(delay = 30.seconds),
      transientErrors = Config.TransientErrorRetries(delay = 1.second, attempts = 5)
    ),
    skipSchemas = List(
      SchemaCriterion.parse("iglu:com.acme/skipped1/jsonschema/1-0-0").get,
      SchemaCriterion.parse("iglu:com.acme/skipped2/jsonschema/1-0-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped3/jsonschema/1-*-*").get,
      SchemaCriterion.parse("iglu:com.acme/skipped4/jsonschema/*-*-*").get
    ),
    telemetry = Telemetry.Config(
      disable         = false,
      interval        = 15.minutes,
      collectorUri    = "collector-g.snowplowanalytics.com",
      collectorPort   = 443,
      secure          = true,
      userProvidedId  = Some("my_pipeline"),
      autoGeneratedId = Some("hfy67e5ydhtrd"),
      instanceId      = Some("665bhft5u6udjf"),
      moduleName      = Some("snowflake-loader-vmss"),
      moduleVersion   = Some("1.0.0")
    ),
    monitoring = Config.Monitoring(
      metrics = Config.Metrics(
        statsd = Some(
          StatsdConfig(
            hostname = "127.0.0.1",
            port     = 8125,
            tags     = Map("myTag" -> "xyz"),
            period   = 1.minute,
            prefix   = "snowplow.snowflake.loader"
          )
        )
      ),
      sentry = Some(Config.SentryM[Id](dsn = "https://public@sentry.example.com/1", tags = Map("myTag" -> "xyz"))),
      healthProbe = Config.HealthProbe(
        port             = Port.fromInt(8000).get,
        unhealthyLatency = 5.minutes
      ),
      webhook = Some(Config.Webhook(endpoint = uri"https://webhook.acme.com", tags = Map("pipeline" -> "production")))
    )
  )
}
