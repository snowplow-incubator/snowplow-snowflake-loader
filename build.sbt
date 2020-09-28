/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
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

lazy val root = project.in(file("."))
  .settings(
    BuildSettings.buildSettings,
    // following lines prevent sbt-dynamodb tasks from being executed for each sub-project
    startDynamoDBLocal / aggregate := false,
    dynamoDBLocalTestCleanup / aggregate := false,
    stopDynamoDBLocal / aggregate := false
  )
  .aggregate(core, loader, transformer)

lazy val core = project.in(file("core"))
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(
    publishArtifact := false,
    libraryDependencies ++= commonDependencies ++ commonTestDependencies
  )

lazy val loader = project.in(file("loader"))
  .dependsOn(core)
  .settings(moduleName := "snowplow-snowflake-loader")
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.snowflakeJdbc,
      Dependencies.ssm,
      Dependencies.sts
    ) ++ commonDependencies ++ commonTestDependencies
  )

lazy val transformer = project.in(file("transformer"))
  .dependsOn(core)
  .settings(moduleName := "snowplow-snowflake-transformer")
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.hadoop,
      Dependencies.hadoopClient,
      Dependencies.spark,
      Dependencies.sparkSql,
      Dependencies.schemaDdl,
      Dependencies.badRows,
      Dependencies.circeJawn,
      Dependencies.circeCore,
      Dependencies.circeOptics,
      Dependencies.circeLiteral
    ) ++ commonTestDependencies
  )

lazy val commonDependencies = Seq(
  // Scala
  Dependencies.analyticsSdk,
  Dependencies.fs2,
  Dependencies.decline,
  Dependencies.s3,
  Dependencies.dynamodb,
  Dependencies.enumeratum,
  Dependencies.igluClient,
  Dependencies.eventsManifest,
)

lazy val commonTestDependencies = Seq(
  Dependencies.specs2,
  Dependencies.specs2Scalacheck,
  Dependencies.scalacheck
)

ThisBuild / useCoursier := false
