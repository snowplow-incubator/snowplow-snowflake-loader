/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

lazy val root = project
  .in(file("."))
  .aggregate(
    streams,
    kafkaLib,
    pubsubLib,
    kinesisLib,
    loadersCommon,
    core,
    kafka,
    kafkaDistroless,
    pubsub,
    pubsubDistroless,
    kinesis,
    kinesisDistroless
  )

/* Common Snowplow internal modules, to become separate library */

lazy val streams: Project = project
  .in(file("snowplow-common-internal/streams-core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kafkaLib: Project = project
  .in(file("snowplow-common-internal/kafka"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaLibDependencies)
  .dependsOn(streams)

lazy val pubsubLib: Project = project
  .in(file("snowplow-common-internal/pubsub"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubLibDependencies)
  .dependsOn(streams)

lazy val kinesisLib: Project = project
  .in(file("snowplow-common-internal/kinesis"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisLibDependencies)
  .dependsOn(streams)
  .settings(
    Defaults.itSettings,
    /**
     * AWS_REGION=eu-central-1 is detected by the lib & integration test suite which follows the
     * same region resolution mechanism as the lib
     */
    IntegrationTest / envVars := Map("AWS_REGION" -> "eu-central-1")
  )
  .configs(IntegrationTest)

lazy val loadersCommon: Project = project
  .in(file("snowplow-common-internal/loaders-common"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.loadersCommonDependencies)

/* End of common Snowplow internal modules */

/* This app */

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)
  .dependsOn(loadersCommon, streams)

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(core, kafkaLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kafkaDistroless: Project = project
  .in(file("modules/distroless/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .dependsOn(core, kafkaLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val pubsub: Project = project
  .in(file("modules/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(core, pubsubLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val pubsubDistroless: Project = project
  .in(file("modules/distroless/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .dependsOn(core, pubsubLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val kinesis: Project = project
  .in(file("modules/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(core, kinesisLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kinesisDistroless: Project = project
  .in(file("modules/distroless/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .dependsOn(core, kinesisLib)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

ThisBuild / fork := true
