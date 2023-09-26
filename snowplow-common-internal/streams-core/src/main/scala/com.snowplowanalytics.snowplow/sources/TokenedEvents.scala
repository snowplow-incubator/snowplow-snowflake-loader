/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources

import cats.effect.kernel.Unique

import java.nio.ByteBuffer

/**
 * The events as they are fed into a [[EventProcessor]]
 *
 * @param events
 *   Each item in the List an event read from the external stream, before parsing
 * @param ack
 *   The [[EventProcessor]] must emit this token after it has fully processed the batch of events.
 *   When the [[EventProcessor]] emits the token, it is an instruction to the [[SourceAndAck]] to
 *   ack/checkpoint the events.
 */
case class TokenedEvents(events: List[ByteBuffer], ack: Unique.Token)
