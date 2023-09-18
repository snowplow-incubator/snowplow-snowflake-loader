/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package net.snowflake.ingest.streaming.internal

import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel
import net.snowflake.ingest.streaming.internal.SnowflakeStreamingIngestChannelInternal

import java.util.concurrent.CompletableFuture

object SnowsFlakePlowInterop {

  /**
   * Flushes the channel
   *
   * The public interface of the Snowflake SDK does not tell us when the events are safely written
   * to Snowflake. So we must cast it to an Internal class so we get access to the `flush()` method.
   */
  def flushChannel(channel: SnowflakeStreamingIngestChannel): CompletableFuture[Void] =
    channel.asInstanceOf[SnowflakeStreamingIngestChannelInternal[_]].flush(false)

}
