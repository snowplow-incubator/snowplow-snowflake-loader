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

case class KafkaSourceConfig(
  topicName: String,
  bootstrapServers: String,
  consumerConf: Map[String, String]
)

object KafkaSourceConfig {
  implicit def decoder: Decoder[KafkaSourceConfig] = deriveDecoder[KafkaSourceConfig]
}
