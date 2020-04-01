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
  .settings(BuildSettings.buildSettings)
  .aggregate(core, loader, transformer)

lazy val core = project
  .settings(moduleName := "snowplow-snowflake-core")
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(libraryDependencies ++= commonDependencies)
  .settings(BuildSettings.dynamoDbSettings)

lazy val loader = project
  .settings(moduleName := "snowplow-snowflake-loader")
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.snowflakeJdbc,
      Dependencies.ssm,
      Dependencies.sts
    ) ++ commonDependencies
  )
  .dependsOn(core)


lazy val transformer = project
  .settings(moduleName := "snowplow-snowflake-transformer")
  .settings(BuildSettings.scalifySettings)
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.dynamoDbSettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
      "Maven Twitter" at "https://maven.twttr.com/" // Used for Hadoop LZO
    ),
    libraryDependencies ++= Seq(
      Dependencies.hadoop,
      Dependencies.hadoopLzo,
      Dependencies.spark,
      Dependencies.sparkSql,
      Dependencies.schemaDdl,
      Dependencies.circeCore,
      Dependencies.circeLiteral
    ) ++ commonDependencies
  )
  .dependsOn(core)

lazy val commonDependencies = Seq(
  // Scala
  Dependencies.analyticsSdk,
  Dependencies.fs2,
  Dependencies.decline,
  Dependencies.declineEnumeratum,
  Dependencies.s3,
  Dependencies.dynamodb,
  Dependencies.enumeratum,
  Dependencies.igluClient,
  Dependencies.eventsManifest,
  // Scala (test-only)
  Dependencies.specs2,
  Dependencies.specs2Scalacheck,
  Dependencies.scalacheck
)

