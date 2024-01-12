/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.Applicative
import cats.effect.Sync
import cats.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import retry._
import retry.implicits.retrySyntaxError
import net.snowflake.ingest.connection.IngestResponseException

import java.lang.SecurityException

import com.snowplowanalytics.snowplow.snowflake.{Alert, AppHealth, Config, Monitoring}

object SnowflakeRetrying {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def withRetries[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert
  )(
    action: F[A]
  ): F[A] =
    retryUntilSuccessful(appHealth, config, monitoring, toAlert, action) <*
      appHealth.setServiceHealth(AppHealth.Service.Snowflake, isHealthy = true)

  private def retryUntilSuccessful[F[_]: Sync: Sleep, A](
    appHealth: AppHealth[F],
    config: Config.Retries,
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert,
    action: F[A]
  ): F[A] =
    action
      .onError(_ => appHealth.setServiceHealth(AppHealth.Service.Snowflake, isHealthy = false))
      .retryingOnSomeErrors(
        isWorthRetrying = isSetupError[F](_),
        policy          = policyForSetupErrors[F](config),
        onError         = logErrorAndSendAlert[F](monitoring, toAlert, _, _)
      )
      .retryingOnAllErrors(
        policy  = policyForTransientErrors[F](config),
        onError = logError[F](_, _)
      )

  /** Is an error associated with setting up Snowflake as a destination */
  private def isSetupError[F[_]: Sync](t: Throwable): F[Boolean] = t match {
    case CausedByIngestResponseException(ire) if ire.getErrorCode === 403 =>
      true.pure[F]
    case _: SecurityException =>
      // Authentication failure, i.e. user unrecognized or bad private key
      true.pure[F]
    case sql: java.sql.SQLException if sql.getErrorCode === 2003 =>
      // Object does not exist or not authorized
      true.pure[F]
    case _ =>
      false.pure[F]
  }

  private def policyForSetupErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.exponentialBackoff[F](config.setupErrors.delay)

  private def policyForTransientErrors[F[_]: Applicative](config: Config.Retries): RetryPolicy[F] =
    RetryPolicies.fullJitter[F](config.transientErrors.delay).join(RetryPolicies.limitRetries(config.transientErrors.attempts - 1))

  private def logErrorAndSendAlert[F[_]: Sync](
    monitoring: Monitoring[F],
    toAlert: Throwable => Alert,
    error: Throwable,
    details: RetryDetails
  ): F[Unit] =
    logError(error, details) *> monitoring.alert(toAlert(error))

  private def logError[F[_]: Sync](error: Throwable, details: RetryDetails): F[Unit] =
    Logger[F].error(error)(s"Executing Snowflake command failed. ${extractRetryDetails(details)}")

  private def extractRetryDetails(details: RetryDetails): String = details match {
    case RetryDetails.GivingUp(totalRetries, totalDelay) =>
      s"Giving up on retrying, total retries: $totalRetries, total delay: ${totalDelay.toSeconds} seconds"
    case RetryDetails.WillDelayAndRetry(nextDelay, retriesSoFar, cumulativeDelay) =>
      s"Will retry in ${nextDelay.toMillis} milliseconds, retries so far: $retriesSoFar, total delay so far: ${cumulativeDelay.toMillis} milliseconds"
  }

  private object CausedByIngestResponseException {
    def unapply(t: Throwable): Option[IngestResponseException] =
      t match {
        case ire: IngestResponseException => Some(ire)
        case _ =>
          Option(t.getCause) match {
            case Some(cause) => unapply(cause)
            case _           => None
          }
      }
  }
}
