/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */

// SBT
import sbt._
import sbt.io.IO
import Keys._

import org.scalafmt.sbt.ScalafmtPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtbuildinfo.BuildInfoPlugin.autoImport._
import sbtdynver.DynVerPlugin.autoImport._
import com.typesafe.sbt.packager.docker.DockerPlugin.autoImport._

import scala.sys.process._

object BuildSettings {

  lazy val commonSettings = Seq(
    organization := "com.snowplowanalytics",
    scalaVersion := "2.13.10",
    scalafmtConfig := file(".scalafmt.conf"),
    scalafmtOnCompile := false,
    scalacOptions += "-Ywarn-macros:after",
    addCompilerPlugin(Dependencies.betterMonadicFor),
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-", // to be compatible with docker

    Compile / resourceGenerators += Def.task {
      val license = (Compile / resourceManaged).value / "META-INF" / "LICENSE"
      IO.copyFile(file("LICENSE.md"), license)
      Seq(license)
    }.taskValue,

    // used in extended configuration parsing unit tests
    Test / envVars := Map(
      "SNOWFLAKE_PRIVATE_KEY" -> "secretPrivateKey",
      "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE" -> "secretKeyPassphrase"
    )
  )

  lazy val appSettings = Seq(
    buildInfoKeys := Seq[BuildInfoKey](dockerAlias, name, version),
    buildInfoPackage := "com.snowplowanalytics.snowplow.snowflake",
    buildInfoOptions += BuildInfoOption.Traits("com.snowplowanalytics.snowplow.runtime.AppInfo")
  ) ++ commonSettings

  lazy val kafkaSettings = appSettings ++ addExampleConfToTestCp ++ Seq(
    name := "snowflake-loader-kafka",
    buildInfoKeys += BuildInfoKey("cloud" -> "Azure")
  )

  lazy val pubsubSettings = appSettings ++ addExampleConfToTestCp ++ Seq(
    name := "snowflake-loader-pubsub",
    buildInfoKeys += BuildInfoKey("cloud" -> "GCP")
  )

  lazy val kinesisSettings = appSettings ++ addExampleConfToTestCp ++ Seq(
    name := "snowflake-loader-kinesis",
    buildInfoKeys += BuildInfoKey("cloud" -> "AWS")
  )

  lazy val addExampleConfToTestCp = Seq(
    Test / unmanagedClasspath += {
      if (baseDirectory.value.getPath.contains("distroless")) {
        // baseDirectory is like 'root/modules/distroless/module',
        // we're at 'module' and need to get to 'root/config/'
        baseDirectory.value.getParentFile.getParentFile.getParentFile / "config"
      } else {
        // baseDirectory is like 'root/modules/module',
        // we're at 'module' and need to get to 'root/config/'
        baseDirectory.value.getParentFile.getParentFile / "config"
      }
    }
  )

}
