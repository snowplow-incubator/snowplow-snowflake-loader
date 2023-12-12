/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake

import cats.effect.{Resource, Sync}
import cats.implicits.catsSyntaxApplyOps
import com.snowplowanalytics.snowplow.runtime.AppInfo
import io.sentry.{Sentry => JSentry, SentryOptions}

object Sentry {

  def capturingAnyException[F[_]: Sync](appInfo: AppInfo, config: Option[Config.Sentry]): Resource[F, Unit] =
    config match {
      case Some(sentryConfig) =>
        initSentry(appInfo, sentryConfig)
      case None =>
        Resource.unit[F]
    }

  private def initSentry[F[_]: Sync](appInfo: AppInfo, sentryConfig: Config.Sentry): Resource[F, Unit] = {
    val acquire = Sync[F].delay(JSentry.init(createSentryOptions(appInfo, sentryConfig)))
    val release = Sync[F].delay(JSentry.close())

    Resource.makeCase(acquire) {
      case (_, Resource.ExitCase.Errored(e)) => Sync[F].delay(JSentry.captureException(e)) *> release
      case _                                 => release

    }
  }

  private def createSentryOptions(appInfo: AppInfo, sentryConfig: Config.Sentry): SentryOptions = {
    val options = new SentryOptions
    options.setDsn(sentryConfig.dsn)
    options.setRelease(appInfo.version)
    sentryConfig.tags.foreach { case (k, v) =>
      options.setTag(k, v)
    }
    options
  }
}
