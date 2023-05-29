/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks

import io.circe.Decoder
import io.circe.generic.semiauto._

case class KafkaSinkConfig(
  topicName: String,
  bootstrapServers: String,
  producerConf: Map[String, String]
)

object KafkaSinkConfig {
  implicit def decoder: Decoder[KafkaSinkConfig] = deriveDecoder[KafkaSinkConfig]
}
