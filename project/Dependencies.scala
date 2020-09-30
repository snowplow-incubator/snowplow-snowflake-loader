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
    val hadoop           = "2.8.5"
    val snowflakeJdbc    = "3.12.10"
    val aws              = "1.11.209"
    // Scala
    val spark            = "2.4.7"
    val fs2              = "2.4.4"
    val decline          = "1.0.0"
    val analyticsSdk     = "2.0.1"
    val enumeratum       = "1.5.13"
    val igluClient       = "1.0.2"
    val eventsManifest   = "0.3.0"
    val badRows          = "2.1.0"
    val schemaDdl        = "0.9.0"
    val circe            = "0.13.0"
    val jackson          = "2.9.10.6"
    // Scala (test only)
    val specs2           = "4.6.0"
    val scalacheck       = "1.14.1"
  }

  // Java
  val hadoop           = "org.apache.hadoop"     % "hadoop-aws"                    % V.hadoop         % Provided
  val hadoopClient     = "org.apache.hadoop"     % "hadoop-mapreduce-client-core"  % V.hadoop         % Provided
  val snowflakeJdbc    = "net.snowflake"         % "snowflake-jdbc"                % V.snowflakeJdbc
  val s3               = "com.amazonaws"         % "aws-java-sdk-s3"               % V.aws
  val dynamodb         = "com.amazonaws"         % "aws-java-sdk-dynamodb"         % V.aws
  val ssm              = "com.amazonaws"         % "aws-java-sdk-ssm"              % V.aws
  val sts              = "com.amazonaws"         % "aws-java-sdk-sts"              % V.aws

  // Scala
  val spark            = "org.apache.spark"      %% "spark-core"                   % V.spark          % Provided
  val sparkSql         = "org.apache.spark"      %% "spark-sql"                    % V.spark          % Provided
  val fs2              = "co.fs2"                %% "fs2-core"                     % V.fs2
  val decline          = "com.monovore"          %% "decline"                      % V.decline
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val enumeratum       = "com.beachape"          %% "enumeratum"                   % V.enumeratum
  val igluClient       = ("com.snowplowanalytics" %% "iglu-scala-client"           % V.igluClient)
    .exclude("com.fasterxml.jackson.core", "jackson-databind")  // Incompatible with Spark
  val jackson          = "com.fasterxml.jackson.core" % "jackson-databind"         % V.jackson
  val eventsManifest   = "com.snowplowanalytics" %% "snowplow-events-manifest"     % V.eventsManifest
  val badRows          = "com.snowplowanalytics" %% "snowplow-badrows"             % V.badRows
  val schemaDdl        = "com.snowplowanalytics" %% "schema-ddl"                   % V.schemaDdl
  val circeJawn        = "io.circe"              %% "circe-jawn"                   % V.circe
  val circeCore        = "io.circe"              %% "circe-core"                   % V.circe          % Test
  val circeOptics      = "io.circe"              %% "circe-optics"                 % V.circe          % Test
  val circeLiteral     = "io.circe"              %% "circe-literal"                % V.circe          % Test


  // Scala (test only)
  val specs2           = "org.specs2"            %% "specs2-core"                  % V.specs2         % Test
  val specs2Scalacheck = "org.specs2"            %% "specs2-scalacheck"            % V.specs2         % Test

  val scalacheck       = "org.scalacheck"        %% "scalacheck"                   % V.scalacheck     % Test
}
