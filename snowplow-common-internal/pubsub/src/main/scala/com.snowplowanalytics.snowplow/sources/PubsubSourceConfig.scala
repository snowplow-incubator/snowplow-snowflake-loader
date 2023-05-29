/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import io.circe.Decoder
import io.circe.generic.semiauto._
import io.circe.config.syntax._

import scala.concurrent.duration.FiniteDuration

case class PubsubSourceConfig(
  subscription: PubsubSourceConfig.Subscription,
  parallelPullCount: Int,
  bufferMaxBytes: Long,
  maxAckExtensionPeriod: FiniteDuration,
  minDurationPerAckExtension: FiniteDuration,
  maxDurationPerAckExtension: FiniteDuration
)

object PubsubSourceConfig {

  case class Subscription(projectId: String, subscriptionId: String)

  private implicit def subscriptionDecoder: Decoder[Subscription] =
    Decoder.decodeString
      .map(_.split("/"))
      .emap {
        case Array("projects", projectId, "subscriptions", subscriptionId) =>
          Right(Subscription(projectId, subscriptionId))
        case _ =>
          Left("Expected format: projects/<project>/subscriptions/<subscription>")
      }

  implicit def decoder: Decoder[PubsubSourceConfig] = deriveDecoder[PubsubSourceConfig]
}
