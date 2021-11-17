/*
 * Copyright (c) 2017-2021 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.transformer

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.snowplowanalytics.snowplow.eventsmanifest.{DynamoDbManifest, EventsManifest, EventsManifestConfig}

/** Singletons needed for unserializable or stateful classes. */
object singleton {

  /** Singleton for EventsManifest to maintain one per node. */
  object EventsManifestSingleton {
    @volatile private var instance: Option[EventsManifest] = _

    /**
      * Retrieve or build an instance of EventsManifest.
      * @param eventsManifestConfig configuration for EventsManifest
      */
    def get(eventsManifestConfig: Option[EventsManifestConfig]): Option[EventsManifest] = {
      eventsManifestConfig match {
        case None => None
        case Some(config) =>
          if (instance == null) {
            synchronized {
              instance = config match {
                case EventsManifestConfig.DynamoDb(_, "local", None, region, table) =>
                  val client = AmazonDynamoDBClientBuilder
                    .standard()
                    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", region))
                    .build()
                  Some(new DynamoDbManifest(client, table))
                case _ => EventsManifest.initStorage(config).fold(exception => throw new RuntimeException(exception.toString), manifest => Some(manifest))
              }
            }
          }
          instance
      }
    }
  }
}
