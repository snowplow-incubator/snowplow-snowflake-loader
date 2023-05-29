/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sinks

/**
 * A single event that can be written to the external sink
 *
 * @param bytes
 *   the serialized content of this event
 * @param partitionKey
 *   optionally controls which partition the event is written to
 * @param attributes
 *   optionally add attributes/headers to the event, if the sink supports this feature
 */
case class Sinkable(
  bytes: Array[Byte],
  partitionKey: Option[String],
  attributes: Map[String, String]
)
