/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.effect.Sync
import cats.effect.std.Random
import cats.implicits._

import scala.concurrent.duration.FiniteDuration

/**
 * Configures how events are fed into the [[EventProcessor]]
 *
 * @note
 *   This class is not for user-facing configuration. The application itself should have an opinion
 *   on these parameters, e.g. Enrich always wants NoWindowing, but Transformer always wants
 *   Windowing
 *
 * @param windowing
 *   Whether to open a new [[EventProcessor]] to handle a timed window of events (e.g. for the
 *   transformer) or whether to feed events to a single [[EventProcessor]] in a continuous endless
 *   stream (e.g. Enrich)
 */
case class EventProcessingConfig(windowing: EventProcessingConfig.Windowing)

object EventProcessingConfig {

  sealed trait Windowing
  case object NoWindowing extends Windowing

  /**
   * Configures windows e.g. for Transformer
   *
   * @param duration
   *   The base level duration between windows
   * @param firstWindowScaling
   *   A random factor to adjust the size of the first window. This addresses the situation where
   *   several parallel instances of the app all start at the same time. All instances in the group
   *   should end windows at slightly different times, so that downstream gets a more steady flow of
   *   completed batches.
   */
  case class TimedWindows(duration: FiniteDuration, firstWindowScaling: Double) extends Windowing

  object TimedWindows {
    def build[F[_]: Sync](duration: FiniteDuration): F[TimedWindows] =
      for {
        random <- Random.scalaUtilRandom
        factor <- random.nextDouble
      } yield TimedWindows(duration, factor)
  }

}
