/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.sources.kinesis

import cats.effect.{IO, Resource}
import org.slf4j.LoggerFactory
import org.testcontainers.containers.localstack.LocalStackContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.regions.Region

object Localstack {

  def resource(region: Region, kinesisInitializeStreams: String): Resource[IO, LocalStackContainer] =
    Resource.make {
      val localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0"))
      localstack.addEnv("AWS_DEFAULT_REGION", region.id)
      localstack.addEnv("KINESIS_INITIALIZE_STREAMS", kinesisInitializeStreams)
      localstack.addExposedPort(4566)
      localstack.setWaitStrategy(Wait.forLogMessage(".*Ready.*", 1))
      IO(startLocalstack(localstack))
    }(ls => IO.blocking(ls.stop()))

  private def startLocalstack(localstack: LocalStackContainer): LocalStackContainer = {
    localstack.start()
    val logger = LoggerFactory.getLogger(KinesisSourceSpec.getClass.getSimpleName)
    val logs   = new Slf4jLogConsumer(logger)
    localstack.followOutput(logs)
    localstack
  }
}
