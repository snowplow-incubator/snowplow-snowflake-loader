/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import fs2.{Chunk, Stream}
import cats.effect.{ExitCode, IO, IOApp, Resource}
import io.circe.literal._
import com.comcast.ip4s.Port
import net.snowflake.ingest.utils.SnowflakeURL

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.snowplow.sources.{EventProcessingConfig, EventProcessor, SourceAndAck, TokenedEvents}
import com.snowplowanalytics.snowplow.sinks.Sink
import com.snowplowanalytics.snowplow.runtime.{AcceptedLicense, AppInfo, HttpClient, Retrying, Telemetry, Webhook}
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Event, SnowplowEvent}

import java.nio.charset.StandardCharsets
import java.nio.ByteBuffer
import java.util.UUID
import java.time.Instant
import scala.concurrent.duration.{DurationLong, FiniteDuration}

object Main extends IOApp {

  type AnyConfig = Config[Any, Any]

  def run(args: List[String]): IO[ExitCode] =
    Run.fromConfig(appInfo, { _: Any => testSource }, { _: Any => logSink }, config)

  def snowflakeUrl: Config.Snowflake.Url = {
    val sdkUrl = new SnowflakeURL(sys.env("SNOWFLAKE_HOST"))
    Config.Snowflake.Url(sdkUrl.getFullUrl, sdkUrl.getJdbcUrl)
  }

  def config: AnyConfig = Config(
    input = (),
    output = Config.Output(
      good = Config.Snowflake(
        url                  = snowflakeUrl,
        user                 = sys.env("SNOWFLAKE_USER"),
        privateKey           = sys.env("SNOWFLAKE_PRIVATE_KEY"),
        privateKeyPassphrase = sys.env.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        role                 = sys.env.get("SNOWFLAKE_ROLE"),
        database             = sys.env("SNOWFLAKE_DATABASE"),
        schema               = sys.env("SNOWFLAKE_SCHEMA"),
        table                = "events",
        channel              = sys.env("SNOWFLAKE_CHANNEL"),
        jdbcLoginTimeout     = 60.seconds,
        jdbcNetworkTimeout   = 60.seconds,
        jdbcQueryTimeout     = 60.seconds
      ),
      bad = Config.SinkWithMaxSize((), Int.MaxValue)
    ),
    batching             = Config.Batching(maxBytes = 1000000, maxDelay = 1.millis, uploadParallelismFactor = BigDecimal(2.5)),
    cpuParallelismFactor = BigDecimal(0.75),
    retries = Config.Retries(
      Retrying.Config.ForSetup(30.seconds),
      Retrying.Config.ForTransient(1.second, 5),
      Config.CheckCommittedOffsetRetries(100.millis)
    ),
    skipSchemas = Nil,
    telemetry   = Telemetry.Config(true, 1.hour, "", 1, false, None, None, None, None, None),
    monitoring = Config.Monitoring(
      metrics     = Config.Metrics(None),
      sentry      = None,
      healthProbe = Config.HealthProbe(Port.fromInt(8082).get, 1.hour),
      webhook     = Webhook.Config(None, Map.empty, 1.hour)
    ),
    http    = Config.Http(HttpClient.Config(2)),
    license = AcceptedLicense()
  )

  private def appInfo: AppInfo = new AppInfo {
    def name: String        = "xyz"
    def version: String     = "xyz"
    def dockerAlias: String = "xyz"
    def cloud: String       = "xyz"
  }

  private def testSource: IO[SourceAndAck[IO]] = IO.delay {
    new SourceAndAck[IO] {
      def stream(config: EventProcessingConfig[IO], processor: EventProcessor[IO]): Stream[IO, Nothing] =
        Stream
          .range(0, 50)
          .evalMap(tokenedEvents(_))
          .append(Stream.eval(specialEvents))
          .append(Stream.range(1000, 1050).evalMap(tokenedEvents(_)))
          .through(processor)
          .drain

      def isHealthy(maxAllowedProcessingLatency: FiniteDuration): IO[SourceAndAck.HealthStatus] =
        IO.pure(SourceAndAck.Healthy)

      def currentStreamLatency: IO[Option[FiniteDuration]] =
        IO.pure(None)
    }
  }

  private def logSink: Resource[IO, Sink[IO]] = Resource.pure {
    Sink[IO] { listOfList =>
      listOfList.traverse_ { sinkable =>
        val str = new String(sinkable.bytes, StandardCharsets.UTF_8)
        IO.println(s"BAD ROW: $str")
      }
    }
  }

  private def events(i: Int): List[Event] =
    (1 to 1000).map { _ =>
      Event
        .minimal(UUID.randomUUID(), Instant.now, "test", "test")
        .copy(app_id = Some(s"ianstr-$i"))
    }.toList

  private def tokenedEvents(i: Int): IO[TokenedEvents] =
    for {
      _ <- IO.sleep(200.millis)
      token <- IO.unique
      chunk = Chunk.from(events(i).map(e => ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))))
    } yield TokenedEvents(chunk, token)

  private def specialEvents: IO[TokenedEvents] =
    tokenedEvents(99999999).map { te =>
      val sdj = SelfDescribingData(
        SchemaKey.fromUri("iglu:myvendor1/myunstruct1/jsonschema/1-0-42").toOption.get,
        json"""{"xyz": "xyz"}"""
      )
      val e = Event
        .minimal(UUID.randomUUID(), Instant.now, "test", "test")
        .copy(app_id = Some("ianstr-99999999"))
        .copy(unstruct_event = SnowplowEvent.UnstructEvent(Some(sdj)))
      val extra = ByteBuffer.wrap(e.toTsv.getBytes(StandardCharsets.UTF_8))
      val chunk = Chunk.from(extra :: te.events.toList)
      te.copy(events = chunk)
    }
}
