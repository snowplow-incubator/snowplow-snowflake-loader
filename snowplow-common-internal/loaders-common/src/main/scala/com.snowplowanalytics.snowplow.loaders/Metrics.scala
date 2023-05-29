/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.effect.{Async, Sync}
import cats.effect.kernel.{Ref, Resource}
import cats.implicits._
import fs2.Stream
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.net.{DatagramPacket, DatagramSocket, InetAddress}
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.{DurationInt, FiniteDuration}

abstract class Metrics[F[_]: Async, S <: Metrics.State](
  ref: Ref[F, S],
  emptyState: S,
  config: Option[Metrics.StatsdConfig]
) {
  def report: Stream[F, Nothing] = {
    val stream = for {
      reporters <- Stream.resource(Metrics.makeReporters[F](config))
      _ <- Stream.fixedDelay[F](config.fold(1.minute)(_.period))
      state <- Stream.eval(ref.getAndSet(emptyState))
      kv = state.toKVMetrics
      _ <- Stream.eval(reporters.traverse(_.report(kv)))
    } yield ()
    stream.drain
  }
}

object Metrics {

  case class StatsdConfig(
    hostname: String,
    port: Int,
    tags: Map[String, String],
    period: FiniteDuration,
    prefix: String
  )

  trait State {
    def toKVMetrics: List[KVMetric]
  }

  sealed trait MetricType {
    def render: Char
  }

  object MetricType {
    case object Gauge extends MetricType { def render = 'g' }
    case object Count extends MetricType { def render = 'c' }
  }

  trait KVMetric {
    def key: String
    def value: String
    def metricType: MetricType
  }

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  private trait Reporter[F[_]] {
    def report(metrics: List[KVMetric]): F[Unit]
  }

  private def stdoutReporter[F[_]: Sync]: Reporter[F] = new Reporter[F] {
    def report(metrics: List[KVMetric]): F[Unit] =
      metrics.traverse_ { kv =>
        Logger[F].info(s"${kv.key} = ${kv.value}")
      }
  }

  private def makeReporters[F[_]: Sync](config: Option[StatsdConfig]): Resource[F, List[Reporter[F]]] =
    config match {
      case None => Resource.pure(List(stdoutReporter[F]))
      case Some(c) =>
        Resource
          .fromAutoCloseable(Sync[F].delay(new DatagramSocket))
          .map { socket =>
            List(stdoutReporter, statsdReporter(c, socket))
          }
    }

  private def statsdReporter[F[_]: Sync](config: StatsdConfig, socket: DatagramSocket): Reporter[F] = new Reporter[F] {

    val tagStr = config.tags.map { case (k, v) => s"$k:$v" }.mkString(",")
    val prefix = config.prefix.stripSuffix(".")

    def report(metrics: List[KVMetric]): F[Unit] =
      Sync[F]
        .blocking(InetAddress.getByName(config.hostname))
        .flatMap { addr =>
          Sync[F].blocking {
            metrics.foreach { kv =>
              val str = s"${prefix}.${kv.key}:${kv.value}|${kv.metricType.render}|#$tagStr".stripPrefix(".")
              val bytes = str.getBytes(UTF_8)
              val packet = new DatagramPacket(bytes, bytes.length, addr, config.port)
              socket.send(packet)
            }
          }
        }
        .recoverWith { t =>
          Logger[F].warn(t)("Caught exception sending statsd metrics")
        }
  }

}
