/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
