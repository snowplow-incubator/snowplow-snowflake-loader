package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.{Concurrent, Ref}
import cats.implicits._
import com.snowplowanalytics.snowplow.runtime.HealthProbe

trait SnowflakeHealth[F[_]] {
  def setUnhealthy(): F[Unit]
  def setHealthy(): F[Unit]
}

object SnowflakeHealth {
  private val unhealthy = HealthProbe.Unhealthy("Snowflake connection is not healthy")

  final case class Stateful[F[_]](state: Ref[F, HealthProbe.Status]) extends SnowflakeHealth[F] {
    def setUnhealthy(): F[Unit] = state.set(unhealthy)
    def setHealthy(): F[Unit]   = state.set(HealthProbe.Healthy)
  }

  def initUnhealthy[F[_]: Concurrent]: F[Stateful[F]] =
    Ref
      .of[F, HealthProbe.Status](unhealthy)
      .map(Stateful.apply)
}
