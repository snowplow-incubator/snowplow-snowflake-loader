/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.loader

import java.time.Instant
import java.util.concurrent.TimeUnit

import cats.{Applicative, Show}
import cats.syntax.show._
import cats.syntax.functor._
import cats.syntax.flatMap._
import cats.effect.{Clock, Sync}

/**
  * Logging capability
  * Something like slf4j isn't used because we cannot pass JVM properties
  * in environments such as EMR
  */
trait Logger[F[_]] {
  /** Always print out to stdout */
  def info[A: Show](msg: => A): F[Unit]
  /** Print out to stdout only if debug output is enabled */
  def debug[A: Show](msg: => A): F[Unit]
  /** Always print out to stderr */
  def error[A: Show](msg: => A): F[Unit]
}

object Logger {

  implicit val instantShow: Show[Instant] =
    Show.fromToString[Instant]

  def apply[F[_]: Logger](implicit ev: Logger[F]): Logger[F] =
    ev

  def initLogger[F[_]: Sync: Clock](debugging: Boolean): Logger[F] = new Logger[F] {
    def info[A: Show](msg: => A): F[Unit] =
      for {
        now <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        message = show"$now: $msg"
        _ <- Sync[F].delay(System.out.println(message))
      } yield ()

    def debug[A: Show](msg: => A): F[Unit] =
      if (debugging)
        for {
          now <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
          message = show"$now DEBUG: $msg"
          _ <- Sync[F].delay(System.out.println(message))
        } yield ()
      else Sync[F].unit

    def error[A: Show](msg: => A): F[Unit] =
      for {
        now <- Clock[F].realTime(TimeUnit.MILLISECONDS).map(Instant.ofEpochMilli)
        message = show"$now ERROR: $msg"
        _ <- Sync[F].delay(System.err.println(message))
      } yield ()
  }

  def initNoop[F[_]: Applicative]: Logger[F] = new Logger[F] {
    def info[A: Show](msg: => A): F[Unit] =
      Applicative[F].unit

    def debug[A: Show](msg: => A): F[Unit] =
      Applicative[F].unit

    def error[A: Show](msg: => A): F[Unit] =
      Applicative[F].unit
  }
}
