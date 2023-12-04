package com.snowplowanalytics.snowplow.snowflake

import cats.implicits._
import cats.{Functor, Monad, Monoid}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.runtime.HealthProbe.{Healthy, Unhealthy}
import com.snowplowanalytics.snowplow.snowflake.processing.SnowflakeHealth
import com.snowplowanalytics.snowplow.sources.SourceAndAck

object AppHealth {

  def isHealthy[F[_]: Monad](
    config: Config.HealthProbe,
    source: SourceAndAck[F],
    snowflakeHealth: SnowflakeHealth[F]
  ): F[HealthProbe.Status] =
    List(
      latencyHealth(config, source),
      snowflakeHealth.state.get
    ).foldA

  private def latencyHealth[F[_]: Functor](config: Config.HealthProbe, source: SourceAndAck[F]): F[HealthProbe.Status] =
    source.processingLatency.map { latency =>
      if (latency > config.unhealthyLatency)
        Unhealthy(show"Processing latency is $latency")
      else
        Healthy
    }

  private val combineHealth: (HealthProbe.Status, HealthProbe.Status) => HealthProbe.Status = {
    case (Healthy, Healthy)                    => Healthy
    case (Healthy, unhealthy)                  => unhealthy
    case (unhealthy, Healthy)                  => unhealthy
    case (Unhealthy(first), Unhealthy(second)) => Unhealthy(reason = s"$first, $second")
  }

  private implicit val healthMonoid: Monoid[HealthProbe.Status] = Monoid.instance(Healthy, combineHealth)
}
