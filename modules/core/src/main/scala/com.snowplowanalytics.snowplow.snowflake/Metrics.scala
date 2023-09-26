/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.implicits._
import fs2.Stream

import com.snowplowanalytics.snowplow.loaders.{Metrics => CommonMetrics}

trait Metrics[F[_]] {
  def addGood(count: Int): F[Unit]
  def addBad(count: Int): F[Unit]

  def report: Stream[F, Nothing]
}

object Metrics {

  def build[F[_]: Async](config: Config.Metrics): F[Metrics[F]] =
    Ref[F].of(State.empty).map(impl(config, _))

  private case class State(
    good: Int,
    bad: Int
  ) extends CommonMetrics.State {
    def toKVMetrics: List[CommonMetrics.KVMetric] =
      List(
        KVMetric.CountGood(good),
        KVMetric.CountBad(bad)
      )
  }

  private object State {
    def empty: State = State(0, 0)
  }

  private def impl[F[_]: Async](config: Config.Metrics, ref: Ref[F, State]): Metrics[F] =
    new CommonMetrics[F, State](ref, State.empty, config.statsd) with Metrics[F] {
      def addGood(count: Int): F[Unit] =
        ref.update(s => s.copy(good = s.good + count))
      def addBad(count: Int): F[Unit] =
        ref.update(s => s.copy(bad = s.bad + count))
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

  }
}
