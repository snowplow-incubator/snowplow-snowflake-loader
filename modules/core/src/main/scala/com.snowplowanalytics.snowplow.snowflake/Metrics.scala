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

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.runtime.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addGood(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]
  def setLatencyMillis(latencyMillis: Long): F[Unit]
  def setLatencyCollectorToTargetMillis(latencyMillis: Long): F[Unit]
  def setLatencyCollectorToTargetPessimisticMillis(latencyMillis: Long): F[Unit]

  def setFailedMaxCollectorTstamp(tstampMillis: Long): F[Unit]
  def clearFailedMaxCollectorTstamp(): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    good: Int,
    bad: Int,
    latencyMillis: Long,
    latencyCollectorToTargetMillis: Option[Long],
    latencyCollectorToTargetPessimisticMillis: Option[Long],
    failedMaxCollectorTstampMillis: Option[Long]
  ) extends CommonMetrics.State {
    private def getLatencyCollectorToTargetMillis: Long = {
      lazy val failedLatency = failedMaxCollectorTstampMillis.map(t => System.currentTimeMillis() - t)
      latencyCollectorToTargetMillis.fold(failedLatency.fold(0L)(identity))(identity)
    }

    private def getLatencyCollectorToTargetPessimisticMillis: Long = {
      lazy val failedLatency = failedMaxCollectorTstampMillis.map(t => System.currentTimeMillis() - t)
      latencyCollectorToTargetPessimisticMillis.fold(failedLatency.fold(0L)(identity))(identity)
    }

    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountGood(good),
        KVMetric.CountBad(bad),
        KVMetric.LatencyMillis(latencyMillis),
        KVMetric.LatencyCollectorToTargetMillis(getLatencyCollectorToTargetMillis),
        KVMetric.LatencyCollectorToTargetPessimisticMillis(getLatencyCollectorToTargetPessimisticMillis)
      )
  }

  private object State {
    def empty: State = State(0, 0, 0L, None, None, None)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, State.empty, config.statsd) with Metrics[F] {
      def addGood(count: Int): F[Unit] =
        ref.update(s => s.copy(good = s.good + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
      def setLatencyMillis(latencyMillis: Long): F[Unit] =
        ref.update(s => s.copy(latencyMillis = s.latencyMillis.max(latencyMillis)))
      def setLatencyCollectorToTargetMillis(latencyMillis: Long): F[Unit] =
        ref.update(s =>
          s.copy(latencyCollectorToTargetMillis = s.latencyCollectorToTargetMillis.fold(latencyMillis)(_.min(latencyMillis)).some)
        )
      def setLatencyCollectorToTargetPessimisticMillis(latencyMillis: Long): F[Unit] =
        ref.update(s =>
          s.copy(latencyCollectorToTargetPessimisticMillis =
            s.latencyCollectorToTargetPessimisticMillis.fold(latencyMillis)(_.max(latencyMillis)).some
          )
        )

      def setFailedMaxCollectorTstamp(tstampMillis: Long): F[Unit] =
        ref.update(s =>
          s.copy(failedMaxCollectorTstampMillis = s.failedMaxCollectorTstampMillis.fold(tstampMillis)(_.max(tstampMillis)).some)
        )
      def clearFailedMaxCollectorTstamp(): F[Unit] =
        ref.update(s => s.copy(failedMaxCollectorTstampMillis = None))
    }

  private object KVMetric {

    final case class CountGood(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_good"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class CountBad(v: Int) extends CommonMetrics.KVMetric {
      val key        = "events_bad"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Count
    }

    final case class LatencyMillis(v: Long) extends CommonMetrics.KVMetric {
      val key        = "latency_millis"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class LatencyCollectorToTargetMillis(v: Long) extends CommonMetrics.KVMetric {
      val key        = "latency_collector_to_target_millis"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }

    final case class LatencyCollectorToTargetPessimisticMillis(v: Long) extends CommonMetrics.KVMetric {
      val key        = "latency_collector_to_target_pessimistic_millis"
      val value      = v.toString
      val metricType = CommonMetrics.MetricType.Gauge
    }
  }
}
