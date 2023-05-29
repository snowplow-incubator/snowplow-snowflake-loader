/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks.internal

import cats.effect.Async
import org.typelevel.log4cats.Logger
import retry._

import com.snowplowanalytics.snowplow.sinks.{Sink, Sinkable}

import scala.concurrent.duration.FiniteDuration

/** This might be not-needed.  Most sinks already have retries built in to the underlying client */
object Retries {

  def sink[F[_]: Async: Logger](maxRetries: Int, baseDelay: FiniteDuration)(f: List[Sinkable] => F[Unit]): Sink[F] = {

    def onError(t: Throwable, details: RetryDetails): F[Unit] = {
      val _ = t // The error can be logged by the surrounding application, not by this lib
      if (details.givingUp)
        Logger[F].warn(s"Error writing batch of events to the output sink. GIVING UP for this batch of events.")
      else
        Logger[F].warn(s"Error writing batch of events to the output sink. RETRYING this batch of events....")
    }

    val policy = RetryPolicies.fullJitter[F](baseDelay).join(RetryPolicies.limitRetries[F](maxRetries))

    Sink[F] { batch =>
      retryingOnAllErrors(policy, onError)(f(batch))
    }
  }

}
