package com.snowplowanalytics.snowplow.snowflake

import cats.effect.{Concurrent, Ref}
import cats.implicits._
import cats.{Monad, Monoid, Show}
import com.snowplowanalytics.snowplow.runtime.HealthProbe
import com.snowplowanalytics.snowplow.runtime.HealthProbe.{Healthy, Unhealthy}
import com.snowplowanalytics.snowplow.sources.SourceAndAck

import scala.concurrent.duration.FiniteDuration

final class AppHealth[F[_]: Monad](
  unhealthyLatency: FiniteDuration,
  source: SourceAndAck[F],
  appManagedServices: Ref[F, Map[AppHealth.Service, Boolean]]
) {

  def status(): F[HealthProbe.Status] =
    for {
      sourceHealth <- getSourceHealth
      servicesHealth <- getAppManagedServicesHealth
    } yield (sourceHealth :: servicesHealth).combineAll

  def setServiceHealth(service: AppHealth.Service, isHealthy: Boolean): F[Unit] =
    appManagedServices.update { currentHealth =>
      currentHealth.updated(service, isHealthy)
    }

  private def getAppManagedServicesHealth: F[List[HealthProbe.Status]] =
    appManagedServices.get.map { services =>
      services.map {
        case (service, false) => HealthProbe.Unhealthy(show"$service is not healthy")
        case _                => HealthProbe.Healthy
      }.toList
    }

  private def getSourceHealth: F[HealthProbe.Status] =
    source.isHealthy(unhealthyLatency).map {
      case SourceAndAck.Healthy              => HealthProbe.Healthy
      case unhealthy: SourceAndAck.Unhealthy => HealthProbe.Unhealthy(unhealthy.show)
    }

  private val combineHealth: (HealthProbe.Status, HealthProbe.Status) => HealthProbe.Status = {
    case (Healthy, Healthy)                    => Healthy
    case (Healthy, unhealthy)                  => unhealthy
    case (unhealthy, Healthy)                  => unhealthy
    case (Unhealthy(first), Unhealthy(second)) => Unhealthy(reason = s"$first, $second")
  }

  private implicit val healthMonoid: Monoid[HealthProbe.Status] = Monoid.instance(Healthy, combineHealth)
}

object AppHealth {

  sealed trait Service

  object Service {
    case object Snowflake extends Service
    case object BadSink extends Service

    implicit val show: Show[Service] = Show.show {
      case Snowflake => "Snowflake"
      case BadSink   => "Bad sink"
    }
  }

  def init[F[_]: Concurrent](
    unhealthyLatency: FiniteDuration,
    source: SourceAndAck[F],
    initialHealth: Map[AppHealth.Service, Boolean]
  ): F[AppHealth[F]] =
    Ref
      .of[F, Map[AppHealth.Service, Boolean]](initialHealth)
      .map(appManaged => new AppHealth[F](unhealthyLatency, source, appManaged))
}
