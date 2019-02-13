/*
 * Copyright (c) 2012-2017 Snowplow Analytics Ltd. All rights reserved.
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
    val snowflakeJdbc    = "3.3.2"
    val aws              = "1.11.209"
    // Scala
    val spark            = "2.2.0"
    val scopt            = "3.7.0"
    val analyticsSdk     = "0.2.1"
    val json4sJackson    = "3.2.11"
    val cats             = "0.9.0"
    val enumeratum       = "1.5.13"
    val igluClient       = "0.5.0"
    val eventsManifest   = "0.1.0"
    // Scala (test only)
    val specs2           = "2.3.13"
    val scalazSpecs2     = "0.2"
    val scalaCheck       = "1.12.2"
  }

  // Java
  val hadoop           = "org.apache.hadoop"     % "hadoop-aws"                    % V.hadoop         % "provided"
  val snowflakeJdbc    = "net.snowflake"         % "snowflake-jdbc"                % V.snowflakeJdbc
  val s3               = "com.amazonaws"         % "aws-java-sdk-s3"               % V.aws
  val dynamodb         = "com.amazonaws"         % "aws-java-sdk-dynamodb"         % V.aws
  val ssm              = "com.amazonaws"         % "aws-java-sdk-ssm"              % V.aws
  val sts              = "com.amazonaws"         % "aws-java-sdk-sts"              % V.aws

  // Scala
  val spark            = "org.apache.spark"      %% "spark-core"                   % V.spark          % "provided"
  val sparkSql         = "org.apache.spark"      %% "spark-sql"                    % V.spark          % "provided"
  val scopt            = "com.github.scopt"      %% "scopt"                        % V.scopt
  val analyticsSdk     = "com.snowplowanalytics" %% "snowplow-scala-analytics-sdk" % V.analyticsSdk
  val json4sJackson    = "org.json4s"            %% "json4s-jackson"               % V.json4sJackson
  val cats             = "org.typelevel"         %% "cats-core"                    % V.cats
  val enumeratum       = "com.beachape"          %% "enumeratum"                   % V.enumeratum
  val igluClient       = "com.snowplowanalytics" %% "iglu-scala-client"            % V.igluClient
  val eventsManifest   = "com.snowplowanalytics" %% "snowplow-events-manifest"     % V.eventsManifest

  // Scala (test only)
  val specs2           = "org.specs2"            %% "specs2"                       % V.specs2         % "test"
  val scalazSpecs2     = "org.typelevel"         %% "scalaz-specs2"                % V.scalazSpecs2   % "test"
  val scalaCheck       = "org.scalacheck"        %% "scalacheck"                   % V.scalaCheck     % "test"
}
