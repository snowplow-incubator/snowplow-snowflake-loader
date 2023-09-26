/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.pubsub

import cats.implicits._
import cats.effect.Async

import com.google.api.core.{ApiFuture, ApiFutureCallback, ApiFutures}
import com.google.common.util.concurrent.MoreExecutors

object FutureInterop {
  def fromFuture[F[_]: Async, A](fut: ApiFuture[A]): F[Unit] =
    Async[F]
      .async[A] { cb =>
        val cancel = Async[F].delay {
          fut.cancel(false): Unit
        }
        Async[F].delay {
          addCallback(fut, cb)
          Some(cancel)
        }
      }
      .void

  private def addCallback[A](fut: ApiFuture[A], cb: Either[Throwable, A] => Unit): Unit = {
    val apiFutureCallback = new ApiFutureCallback[A] {
      def onFailure(t: Throwable): Unit = cb(Left(t))
      def onSuccess(result: A): Unit    = cb(Right(result))
    }
    ApiFutures.addCallback(fut, apiFutureCallback, MoreExecutors.directExecutor)
  }

}
