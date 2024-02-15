/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import scala.reflect._

import com.snowplowanalytics.snowplow.sources.kafka.{KafkaSource, KafkaSourceConfig}
import com.snowplowanalytics.snowplow.sinks.kafka.{KafkaSink, KafkaSinkConfig}
import com.snowplowanalytics.snowplow.azure.AzureAuthenticationCallbackHandler

// We need separate instances of callback handler with separate source and
// sinks because they need different tokens to authenticate. However we are
// only giving class name to Kafka and it initializes the class itself and if
// we pass same class name for all source and sinks, Kafka initializes and uses
// only one instance of the callback handler. To create separate instances, we
// created multiple different classes and pass their names to respective sink
// and source properties. With this way, all the source and sinks will have their
// own callback handler instance.

class SourceAuthHandler extends AzureAuthenticationCallbackHandler

class SinkAuthHandler extends AzureAuthenticationCallbackHandler

object AzureApp extends LoaderApp[KafkaSourceConfig, KafkaSinkConfig](BuildInfo) {

  override def source: SourceProvider = KafkaSource.build(_, classTag[SourceAuthHandler])

  override def badSink: SinkProvider = KafkaSink.resource(_, classTag[SinkAuthHandler])
}
