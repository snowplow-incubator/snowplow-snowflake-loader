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

// SBT
import sbt._
import Keys._

// sbt-assembly
import sbtassembly._
import sbtassembly.AssemblyKeys._
import com.eed3si9n.jarjarabrams.ShadeRule

import sbtdynver.DynVerPlugin.autoImport._

/**
 * Common settings-patterns for Snowplow apps and libraries.
 * To enable any of these you need to explicitly add Settings value to build.sbt
 */
object BuildSettings {

  lazy val buildSettings = Seq[Setting[_]](
    organization := "com.snowplowanalytics",
    scalaVersion := "2.12.12",
    Global / concurrentRestrictions += Tags.limit(Tags.Test, 1),

    javacOptions := Seq(
      "-source", "1.8",
      "-target", "1.8",
      "-Xlint"
    )
  )

  // Makes package (build) metadata available withing source code
  lazy val scalifySettings = Seq(
    Compile / sourceGenerators += Def.task {
      val file = (Compile / sourceManaged).value / "settings.scala"
      IO.write(file, """package com.snowplowanalytics.snowflake.generated
                       |object ProjectMetadata {
                       |  val version = "%s"
                       |  val name = "%s"
                       |  val organization = "%s"
                       |  val scalaVersion = "%s"
                       |}
                       |""".stripMargin.format(version.value, name.value, organization.value, scalaVersion.value))
      Seq(file)
    }.taskValue
  )

  // sbt-assembly settings
  lazy val assemblySettings = Seq(
    assembly / target := file("target/scala-2.12/assembled_jars/"),
    assembly / assemblyJarName := { moduleName.value + "-" + version.value + ".jar" },

    assembly / assemblyShadeRules := Seq(
      ShadeRule.rename(
        "com.amazonaws.**" -> "shadeaws.@1",
        "org.apache.http.**" -> "shadehttp.@1",

       "cats.**" -> "shadecats.@1",
       "shapeless.**" -> "shadeshapeless.@1"
      ).inAll
    ),

    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      val excludes = Set(
        "jasper-compiler-5.5.12.jar",
        "hadoop-core-1.1.2.jar", // Provided by Amazon EMR. Delete this line if you're not on EMR
        "hadoop-tools-1.1.2.jar" // "
      )
      cp.filter { jar => excludes(jar.data.getName) }
    },

    assembly / assemblyMergeStrategy := {
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.startsWith("mime.types") => MergeStrategy.first
      case "module-info.class" => MergeStrategy.discard
      case x if x.endsWith(".html") => MergeStrategy.discard
      case x if x.endsWith("public-suffix-list.txt") => MergeStrategy.last
      case PathList("org", "apache", "spark", "unused", tail@_*) => MergeStrategy.first
      case PathList("com", "github", "fge", tail@_*) => MergeStrategy.first
      case x =>
        val oldStrategy = (assembly / assemblyMergeStrategy).value
        oldStrategy(x)
    }
  )

  lazy val dynVerSettings = Seq(
    ThisBuild / dynverVTagPrefix := false, // Otherwise git tags required to have v-prefix
    ThisBuild / dynverSeparator := "-" // to be compatible with docker
  )
}
