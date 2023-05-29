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
    core,
    kafka,
    kafkaDistroless,
    pubsub,
    pubsubDistroless,
    kinesis,
    kinesisDistroless
  )

lazy val core: Project = project
  .in(file("modules/core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.coreDependencies)

lazy val kafka: Project = project
  .in(file("modules/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kafkaDistroless: Project = project
  .in(file("modules/distroless/kafka"))
  .settings(BuildSettings.kafkaSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .settings(sourceDirectory := (kafka / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val pubsub: Project = project
  .in(file("modules/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val pubsubDistroless: Project = project
  .in(file("modules/distroless/pubsub"))
  .settings(BuildSettings.pubsubSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .settings(sourceDirectory := (pubsub / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val kinesis: Project = project
  .in(file("modules/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val kinesisDistroless: Project = project
  .in(file("modules/distroless/kinesis"))
  .settings(BuildSettings.kinesisSettings)
  .settings(libraryDependencies ++= Dependencies.kinesisDependencies)
  .settings(sourceDirectory := (kinesis / sourceDirectory).value)
  .dependsOn(core)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

ThisBuild / fork := true
