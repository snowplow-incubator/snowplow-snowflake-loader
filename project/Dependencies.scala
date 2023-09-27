/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val cats             = "2.9.0"
    val catsEffect       = "3.5.0"
    val catsRetry        = "3.1.0"
    val fs2              = "3.7.0"
    val log4cats         = "2.6.0"
    val http4s           = "0.23.15"
    val decline          = "2.4.1"
    val circe            = "0.14.3"
    val circeConfig      = "0.10.0"
    val betterMonadicFor = "0.3.1"
    val doobie           = "1.0.0-RC4"

    // Streams
    val fs2Kafka      = "3.0.1"
    val pubsub        = "1.123.17"
    val fs2AwsKinesis = "6.0.3"
    val awsSdk2       = "2.20.135"

    // java
    val slf4j     = "2.0.7"
    val azureSdk  = "1.9.1"
    val sentry    = "6.25.2"
    val snowflake = "2.0.3"
    val jaxb      = "2.3.1"

    // Snowplow
    val schemaDdl  = "0.21.0-M1"
    val badrows    = "2.2.0"
    val igluClient = "3.0.0"
    val tracker    = "2.0.0"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"
    val localstack       = "1.19.0"

  }

  val catsEffectKernel  = "org.typelevel"    %% "cats-effect-kernel"   % V.catsEffect
  val cats              = "org.typelevel"    %% "cats-core"            % V.cats
  val fs2               = "co.fs2"           %% "fs2-core"             % V.fs2
  val log4cats          = "org.typelevel"    %% "log4cats-slf4j"       % V.log4cats
  val catsRetry         = "com.github.cb372" %% "cats-retry"           % V.catsRetry
  val blazeClient       = "org.http4s"       %% "http4s-blaze-client"  % V.http4s
  val decline           = "com.monovore"     %% "decline-effect"       % V.decline
  val circeConfig       = "io.circe"         %% "circe-config"         % V.circeConfig
  val circeGeneric      = "io.circe"         %% "circe-generic"        % V.circe
  val circeGenericExtra = "io.circe"         %% "circe-generic-extras" % V.circe
  val circeLiteral      = "io.circe"         %% "circe-literal"        % V.circe
  val betterMonadicFor  = "com.olegpy"       %% "better-monadic-for"   % V.betterMonadicFor
  val doobie            = "org.tpolecat"     %% "doobie-core"          % V.doobie

  // streams
  val fs2Kafka = "com.github.fd4s" %% "fs2-kafka"           % V.fs2Kafka
  val pubsub   = "com.google.cloud" % "google-cloud-pubsub" % V.pubsub
  val fs2AwsKinesis = ("io.laserdisc" %% "fs2-aws-kinesis" % V.fs2AwsKinesis)
    .exclude("com.amazonaws", "amazon-kinesis-producer")
    .exclude("software.amazon.glue", "schema-registry-build-tools")
    .exclude("software.amazon.glue", "schema-registry-common")
    .exclude("software.amazon.glue", "schema-registry-serde")
  val arnsSdk2            = "software.amazon.awssdk" % "arns"                       % V.awsSdk2
  val kinesisSdk2         = "software.amazon.awssdk" % "kinesis"                    % V.awsSdk2
  val dynamoDbSdk2        = "software.amazon.awssdk" % "dynamodb"                   % V.awsSdk2
  val cloudwatchSdk2      = "software.amazon.awssdk" % "cloudwatch"                 % V.awsSdk2
  val catsEffectTestingIt = "org.typelevel"         %% "cats-effect-testkit"        % V.catsEffect       % IntegrationTest
  val catsEffectSpecs2It  = "org.typelevel"         %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % IntegrationTest
  val localstackIt        = "org.testcontainers"     % "localstack"                 % V.localstack       % IntegrationTest
  val slf4jIt             = "org.slf4j"              % "slf4j-simple"               % V.slf4j            % IntegrationTest

  // java
  val slf4j           = "org.slf4j"      % "slf4j-simple"         % V.slf4j
  val azureIdentity   = "com.azure"      % "azure-identity"       % V.azureSdk
  val sentry          = "io.sentry"      % "sentry"               % V.sentry
  val snowflakeIngest = "net.snowflake"  % "snowflake-ingest-sdk" % V.snowflake
  val jaxb            = "javax.xml.bind" % "jaxb-api"             % V.jaxb

  // snowplow: Note jackson-databind 2.14.x is incompatible with Spark
  val badrows     = "com.snowplowanalytics" %% "snowplow-badrows"                      % V.badrows
  val tracker     = "com.snowplowanalytics" %% "snowplow-scala-tracker-core"           % V.tracker
  val trackerEmit = "com.snowplowanalytics" %% "snowplow-scala-tracker-emitter-http4s" % V.tracker
  val schemaDdl   = "com.snowplowanalytics" %% "schema-ddl"                            % V.schemaDdl
  val igluClient  = "com.snowplowanalytics" %% "iglu-scala-client"                     % V.igluClient

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test

  val streamsDependencies = Seq(
    cats,
    catsEffectKernel,
    catsRetry,
    fs2,
    log4cats,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Provided
  )

  val kafkaLibDependencies = Seq(
    fs2Kafka,
    circeConfig,
    circeGeneric
  )

  val pubsubLibDependencies = Seq(
    pubsub,
    circeConfig,
    circeGeneric
  )

  val kinesisLibDependencies = Seq(
    fs2AwsKinesis,
    arnsSdk2,
    kinesisSdk2,
    dynamoDbSdk2,
    cloudwatchSdk2,
    circeConfig,
    circeGeneric,
    circeGenericExtra,
    circeLiteral % Test,
    catsEffectTestingIt,
    catsEffectSpecs2It,
    localstackIt,
    slf4jIt,
    specs2
  )

  val loadersCommonDependencies = Seq(
    cats,
    catsEffectKernel,
    schemaDdl,
    badrows,
    circeConfig,
    fs2,
    igluClient,
    log4cats,
    tracker,
    trackerEmit,
    specs2,
    catsEffectSpecs2,
    slf4j % Test
  )

  val coreDependencies = Seq(
    catsRetry,
    blazeClient,
    decline,
    sentry,
    snowflakeIngest,
    doobie,
    circeGenericExtra,
    specs2,
    catsEffectSpecs2,
    slf4j % Test
  )

  val kafkaDependencies = Seq(
    slf4j % Runtime,
    jaxb  % Runtime,
    azureIdentity
  )

  val pubsubDependencies = Seq(
    jaxb  % Runtime,
    slf4j % Runtime
  )

  val kinesisDependencies = Seq(
    jaxb  % Runtime,
    slf4j % Runtime
  )

}
