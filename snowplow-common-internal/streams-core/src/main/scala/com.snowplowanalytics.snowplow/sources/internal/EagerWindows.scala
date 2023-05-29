/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.internal

import cats.implicits._
import cats.effect.Concurrent
import cats.effect.kernel.Deferred
import fs2.{Pipe, Stream}

private[internal] object EagerWindows {

  sealed trait EagerWindowResult
  case object PreviousWindowFailed extends EagerWindowResult
  case object PreviousWindowSuccess extends EagerWindowResult

  trait Control[F[_]] {
    def unblockNextWindow(result: EagerWindowResult): F[Unit]
    def waitForPreviousWindow: F[Unit]
  }

  /**
   * This is the machinery which allows us to run multiple windows in parallel (slightly
   * overlapping)
   *
   * The second window is allowed to start processing before the first window has finalized. This is
   * helpful for maximizing usage of the available CPU.
   *
   * But the second window is forbidden from checkpointing until the previous window completes with
   * success. This prevents the second window from checkpointing earlier events which are not yet
   * safely committed to the output.
   */
  def pipes[F[_]: Concurrent, A, B](toPipe: Control[F] => Pipe[F, A, B]): Stream[F, Pipe[F, A, B]] =
    controls.map(toPipe)

  private def controls[F[_]: Concurrent]: Stream[F, Control[F]] =
    Stream
      .eval(Deferred[F, EagerWindowResult])
      .repeat
      .zipWithPrevious
      .map {
        case (Some(previous), next) =>
          new Control[F] {
            def unblockNextWindow(result: EagerWindowResult): F[Unit] =
              next.complete(result).void
            def waitForPreviousWindow: F[Unit] =
              previous.get.flatMap {
                case PreviousWindowFailed =>
                  Concurrent[F].raiseError(new RuntimeException("Eager window abandoned because previous window failed"))
                case PreviousWindowSuccess =>
                  Concurrent[F].unit
              }
          }
        case (None, next) =>
          new Control[F] {
            def unblockNextWindow(result: EagerWindowResult): F[Unit] =
              next.complete(result).void
            def waitForPreviousWindow: F[Unit] =
              Concurrent[F].unit
          }
      }

}
