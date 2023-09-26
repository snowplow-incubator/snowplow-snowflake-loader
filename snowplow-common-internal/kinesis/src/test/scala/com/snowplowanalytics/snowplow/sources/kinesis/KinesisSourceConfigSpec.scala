/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import io.circe.literal._
import org.specs2.Specification

class KinesisSourceConfigSpec extends Specification {

  def is = s2"""
  The KinesisSource decoder should:
    Decode a valid JSON config with PascalCase type hints $e1
    Decode a valid JSON config with CAPITALIZED type hints $e2
  """

  def e1 = {
    val json = json"""
    {
      "appName": "my-app",
      "streamName": "my-stream",
      "retrievalMode": {
        "type": "Polling",
        "maxRecords": 42
      },
      "initialPosition": {
        "type": "TrimHorizon"
      },
      "bufferSize": 42
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.bufferSize.value must beEqualTo(42)
      ).reduce(_ and _)
    }
  }

  def e2 = {
    val json = json"""
    {
      "appName": "my-app",
      "streamName": "my-stream",
      "retrievalMode": {
        "type": "POLLING",
        "maxRecords": 42
      },
      "initialPosition": {
        "type": "TRIM_HORIZON"
      },
      "bufferSize": 42
    }
    """

    json.as[KinesisSourceConfig] must beRight.like { case c: KinesisSourceConfig =>
      List(
        c.appName must beEqualTo("my-app"),
        c.streamName must beEqualTo("my-stream"),
        c.initialPosition must beEqualTo(KinesisSourceConfig.InitialPosition.TrimHorizon),
        c.retrievalMode must beEqualTo(KinesisSourceConfig.Retrieval.Polling(42)),
        c.bufferSize.value must beEqualTo(42)
      ).reduce(_ and _)
    }
  }

}
