package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.{Concurrent, Ref}
import cats.implicits._
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.snowflake.processing.SnowflakeHealth.unhealthy

final case class SnowflakeHealth[F[_]](state: Ref[F, HealthProbe.Status]) {
  def setUnhealthy(): F[Unit] = state.set(unhealthy)
  def setHealthy(): F[Unit]   = state.set(HealthProbe.Healthy)
}

object SnowflakeHealth {
  private val unhealthy = HealthProbe.Unhealthy("Snowflake connection is not healthy")

  def initUnhealthy[F[_]: Concurrent]: F[SnowflakeHealth[F]] =
    Ref
      .of[F, HealthProbe.Status](unhealthy)
      .map(SnowflakeHealth.apply)
}
