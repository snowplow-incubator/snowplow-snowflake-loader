/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.Sync

import eu.timepit.refined.types.all.PosInt

import io.circe._
import io.circe.generic.extras.semiauto.deriveConfiguredDecoder
import io.circe.generic.extras.Configuration

import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain

import java.net.URI
import java.time.Instant

case class KinesisSourceConfig(
  appName: String,
  streamName: String,
  initialPosition: KinesisSourceConfig.InitialPosition,
  retrievalMode: KinesisSourceConfig.Retrieval,
  bufferSize: PosInt,
  customEndpoint: Option[URI],
  dynamodbCustomEndpoint: Option[URI],
  cloudwatchCustomEndpoint: Option[URI]
)

object KinesisSourceConfig {

  private implicit val posIntDecoder: Decoder[PosInt] = Decoder.decodeInt.emap(PosInt.from)

  private[kinesis] def getRuntimeRegion[F[_]: Sync]: F[Region] =
    Sync[F].blocking((new DefaultAwsRegionProviderChain).getRegion)

  sealed trait InitialPosition

  object InitialPosition {
    case object Latest extends InitialPosition
    case object TrimHorizon extends InitialPosition
    case class AtTimestamp(timestamp: Instant) extends InitialPosition

    private[KinesisSourceConfig] def decoder(implicit c: Configuration) = deriveConfiguredDecoder[InitialPosition]
  }

  sealed trait Retrieval

  object Retrieval {
    case class Polling(maxRecords: Int) extends Retrieval
    case object FanOut extends Retrieval

    private[KinesisSourceConfig] def decoder(implicit c: Configuration) = deriveConfiguredDecoder[Retrieval]
  }

  implicit val decoder: Decoder[KinesisSourceConfig] = {
    implicit val config: Configuration = Configuration.default.withDiscriminator("type")
    val alternativeConfig              = config.withScreamingSnakeCaseConstructorNames

    implicit val initPosDecoder = InitialPosition.decoder(config).or(InitialPosition.decoder(alternativeConfig))
    implicit val retrieval      = Retrieval.decoder(config).or(Retrieval.decoder(alternativeConfig))
    deriveConfiguredDecoder[KinesisSourceConfig]
  }
}
