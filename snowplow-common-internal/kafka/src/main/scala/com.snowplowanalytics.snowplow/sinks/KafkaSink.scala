/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks

import cats.implicits._
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.Monad
import fs2.Chunk
import fs2.kafka.{Header, Headers, KafkaProducer, ProducerRecord, ProducerSettings}

import java.util.UUID

object KafkaSink {

  def resource[F[_]: Async](config: KafkaSinkConfig): Resource[F, Sink[F]] = {
    val producerSettings =
      ProducerSettings[F, String, Array[Byte]]
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(config.producerConf)
        .withProperties(
          ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"),
          ("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
        )

    for {
      producer <- KafkaProducer[F].resource(producerSettings)
    } yield fromFs2Producer(config, producer)

  }

  private def fromFs2Producer[F[_]: Monad](config: KafkaSinkConfig, producer: KafkaProducer[F, String, Array[Byte]]): Sink[F] =
    Sink { batch =>
      val records = Chunk.seq(batch.map(toProducerRecord(config, _)))
      producer.produce(records).flatten.void
    }

  private def toProducerRecord(config: KafkaSinkConfig, sinkable: Sinkable): ProducerRecord[String, Array[Byte]] = {
    val headers = Headers.fromIterable {
      sinkable.attributes.map { case (k, v) =>
        Header(k, v)
      }
    }
    ProducerRecord(config.topicName, sinkable.partitionKey.getOrElse(UUID.randomUUID.toString), sinkable.bytes)
      .withHeaders(headers)
  }
}
