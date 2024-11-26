/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
