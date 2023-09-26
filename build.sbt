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
    kafka,
    pubsub,
    loadersCommon,
    core,
    azure,
    azureDistroless,
    gcp,
    gcpDistroless
  )

/* Common Snowplow internal modules, to become separate library */

lazy val streams: Project = project
  .in(file("snowplow-common-internal/streams-core"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.streamsDependencies)

lazy val kafka: Project = project
  .in(file("snowplow-common-internal/kafka"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.kafkaDependencies)
  .dependsOn(streams)

lazy val pubsub: Project = project
  .in(file("snowplow-common-internal/pubsub"))
  .settings(BuildSettings.commonSettings)
  .settings(libraryDependencies ++= Dependencies.pubsubDependencies)
  .dependsOn(streams)

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

lazy val azure: Project = project
  .in(file("modules/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .dependsOn(core, kafka)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val azureDistroless: Project = project
  .in(file("modules/distroless/azure"))
  .settings(BuildSettings.azureSettings)
  .settings(libraryDependencies ++= Dependencies.azureDependencies)
  .settings(sourceDirectory := (azure / sourceDirectory).value)
  .dependsOn(core, kafka)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

lazy val gcp: Project = project
  .in(file("modules/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .dependsOn(core, pubsub)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDockerPlugin)

lazy val gcpDistroless: Project = project
  .in(file("modules/distroless/gcp"))
  .settings(BuildSettings.gcpSettings)
  .settings(libraryDependencies ++= Dependencies.gcpDependencies)
  .settings(sourceDirectory := (gcp / sourceDirectory).value)
  .dependsOn(core, pubsub)
  .enablePlugins(BuildInfoPlugin, JavaAppPackaging, SnowplowDistrolessDockerPlugin)

ThisBuild / fork := true
