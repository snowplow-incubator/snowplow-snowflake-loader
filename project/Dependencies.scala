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
import sbt._

object Dependencies {

  object V {
    // Java
    val hadoop           = "3.2.1"
    val snowflakeJdbc    = "3.13.5"
    val aws              = "1.11.870"
    // Scala
    val spark            = "3.1.2"
    val fs2              = "2.5.6"
    val decline          = "1.4.0"
    val analyticsSdk     = "2.1.0"
    val enumeratum       = "1.7.0"
    val igluClient       = "1.1.1"
    val eventsManifest   = "0.3.0"
    val badRows          = "2.1.1"
    val schemaDdl        = "0.13.0"
    val circe            = "0.14.1"
    val jacksonCbor      = "2.11.4" // Override provided version to fix security vulnerability
    // Scala (test only)
    val specs2           = "4.12.0"
    val scalacheck       = "1.15.4"
  }

  // Java
  val hadoop           = "org.apache.hadoop"                % "hadoop-aws"                    % V.hadoop         % Provided
  val hadoopCommon     = "org.apache.hadoop"                % "hadoop-common"                 % V.hadoop         % Provided
  val hadoopClient     = "org.apache.hadoop"                % "hadoop-mapreduce-client-core"  % V.hadoop         % Provided
  val snowflakeJdbc    = "net.snowflake"                    % "snowflake-jdbc"                % V.snowflakeJdbc
  val s3               = "com.amazonaws"                    % "aws-java-sdk-s3"               % V.aws
  val dynamodb         = "com.amazonaws"                    % "aws-java-sdk-dynamodb"         % V.aws
  val ssm              = "com.amazonaws"                    % "aws-java-sdk-ssm"              % V.aws
  val sts              = "com.amazonaws"                    % "aws-java-sdk-sts"              % V.aws
  val jacksonCbor      = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-cbor"       % V.jacksonCbor excludeAll(
      // Prevent upgrading jackson core libs to incompatible version
      ExclusionRule(organization="com.fasterxml.jackson.core")
    )

  // Scala
  val spark            = "org.apache.spark"      %% "spark-core"                   % V.spark          % Provided
  val sparkSql         = "org.apache.spark"      %% "spark-sql"                    % V.spark          % Provided
  val fs2              = "co.fs2"                %% "fs2-core"                     % V.fs2
  val decline          = "com.monovore"          %% "decline"                      % V.decline
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val enumeratum       = "com.beachape"          %% "enumeratum"                   % V.enumeratum
  val igluClient       = "com.snowplowanalytics" %% "iglu-scala-client"           % V.igluClient
  val eventsManifest   = "com.snowplowanalytics" %% "snowplow-events-manifest"     % V.eventsManifest
  val badRows          = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val schemaDdl        = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val circeJawn        = "io.circe"              %% "circe-jawn"                   % V.circe          % Test
  val circeCore        = "io.circe"              %% "circe-core"                   % V.circe          % Test
  val circeOptics      = "io.circe"              %% "circe-optics"                 % V.circe          % Test
  val circeLiteral     = "io.circe"              %% "circe-literal"                % V.circe          % Test


  // Scala (test only)
  val specs2           = "org.specs2"            %% "specs2-core"                  % V.specs2         % Test
  val specs2Scalacheck = "org.specs2"            %% "specs2-scalacheck"            % V.specs2         % Test

  val scalacheck       = "org.scalacheck"        %% "scalacheck"                   % V.scalacheck     % Test
}
