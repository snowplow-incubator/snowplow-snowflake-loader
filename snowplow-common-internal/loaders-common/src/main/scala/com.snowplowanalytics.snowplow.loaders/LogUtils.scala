/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import org.typelevel.log4cats.Logger

object LogUtils {

  def prettyLogException[F[_]: Logger](e: Throwable): F[Unit] = {

    def causes(e: Throwable): List[String] =
      Option(e.getCause) match {
        case Some(e) => s"caused by: ${e.getMessage}" :: causes(e)
        case None => Nil
      }

    val msg = e.getMessage :: causes(e)
    Logger[F].error(msg.mkString("\n"))
  }

}
