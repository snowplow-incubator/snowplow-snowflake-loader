/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Ref}

import eu.timepit.refined.types.numeric.PosInt

import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient
import software.amazon.awssdk.services.kinesis.model.{PutRecordRequest, PutRecordResponse}

import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.UUID

import com.snowplowanalytics.snowplow.sources.{EventProcessor, TokenedEvents}

object Utils {

  def putDataToKinesis(
    client: KinesisAsyncClient,
    streamName: String,
    data: String
  ): IO[PutRecordResponse] = {
    val record = PutRecordRequest
      .builder()
      .streamName(streamName)
      .data(SdkBytes.fromUtf8String(data))
      .partitionKey(UUID.randomUUID().toString)
      .build()

    IO.blocking(client.putRecord(record).get())
  }

  def getKinesisConfig(endpoint: URI)(streamName: String): KinesisSourceConfig = KinesisSourceConfig(
    UUID.randomUUID().toString,
    streamName,
    KinesisSourceConfig.InitialPosition.TrimHorizon,
    KinesisSourceConfig.Retrieval.Polling(1),
    PosInt.unsafeFrom(1),
    Some(endpoint),
    Some(endpoint),
    Some(endpoint)
  )

  def testProcessor(ref: Ref[IO, List[String]]): EventProcessor[IO] =
    _.evalMap { case TokenedEvents(events, token) =>
      for {
        _ <- ref.update(_ ::: events.map(byteBuffer => StandardCharsets.UTF_8.decode(byteBuffer).toString))
      } yield token
    }

  def getKinesisClient(endpoint: URI, region: Region): IO[KinesisAsyncClient] =
    IO(
      KinesisAsyncClient
        .builder()
        .endpointOverride(endpoint)
        .region(region)
        .build()
    )
}
