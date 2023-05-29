/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import com.snowplowanalytics.snowplow.sources.pubsub.{PubsubSource, PubsubSourceConfig}
import com.snowplowanalytics.snowplow.sinks.pubsub.{PubsubSink, PubsubSinkConfig}

object GcpApp extends LoaderApp[PubsubSourceConfig, PubsubSinkConfig](BuildInfo) {

  override def source: SourceProvider = PubsubSource.build(_)

  override def badSink: SinkProvider = PubsubSink.resource(_)
}
