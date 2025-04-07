/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
import sbt._

object Dependencies {

  object V {
    // Scala
    val catsEffect       = "3.5.0"
    val http4s           = "0.23.15"
    val decline          = "2.4.1"
    val circe            = "0.14.3"
    val betterMonadicFor = "0.3.1"
    val doobie           = "1.0.0-RC4"

    // java
    val slf4j     = "2.0.7"
    val azureSdk  = "1.15.3"
    val sentry    = "6.25.2"
    val snowflake = "3.1.2"
    val sfJDBC    = "3.23.2" // Version override
    val jaxb      = "2.3.1"
    val awsSdk2   = "2.30.17"
    val netty     = "4.1.118.Final" // Version override
    val reactor   = "1.0.39" // Version override
    val snappy    = "1.1.10.4" // Version override
    val nimbusJwt = "9.37.2" // Version override
    val jackson   = "2.15.0" // Version override
    val protobuf  = "3.25.5" // Version override
    val jsonSmart = "2.5.2" // Version override

    // Snowplow
    val streams = "0.11.0"

    // tests
    val specs2           = "4.20.0"
    val catsEffectSpecs2 = "1.5.0"

  }

  val http4sCirce       = "org.http4s"   %% "http4s-circe"         % V.http4s
  val decline           = "com.monovore" %% "decline-effect"       % V.decline
  val circeGenericExtra = "io.circe"     %% "circe-generic-extras" % V.circe
  val betterMonadicFor  = "com.olegpy"   %% "better-monadic-for"   % V.betterMonadicFor
  val doobie            = "org.tpolecat" %% "doobie-core"          % V.doobie

  // java
  val slf4j           = "org.slf4j"                  % "slf4j-simple"         % V.slf4j
  val azureIdentity   = "com.azure"                  % "azure-identity"       % V.azureSdk
  val sentry          = "io.sentry"                  % "sentry"               % V.sentry
  val snowflakeIngest = "net.snowflake"              % "snowflake-ingest-sdk" % V.snowflake
  val snowflakeJDBC   = "net.snowflake"              % "snowflake-jdbc"       % V.sfJDBC
  val jaxb            = "javax.xml.bind"             % "jaxb-api"             % V.jaxb
  val stsSdk2         = "software.amazon.awssdk"     % "sts"                  % V.awsSdk2
  val nettyCodecHttp  = "io.netty"                   % "netty-codec-http2"    % V.netty
  val reactorNetty    = "io.projectreactor.netty"    % "reactor-netty-http"   % V.reactor
  val snappyJava      = "org.xerial.snappy"          % "snappy-java"          % V.snappy
  val nimbusJoseJwt   = "com.nimbusds"               % "nimbus-jose-jwt"      % V.nimbusJwt
  val jacksonCore     = "com.fasterxml.jackson.core" % "jackson-core"         % V.jackson
  val protobufJava    = "com.google.protobuf"        % "protobuf-java"        % V.protobuf
  val protobufUtil    = "com.google.protobuf"        % "protobuf-java-util"   % V.protobuf
  val jsonSmart       = "net.minidev"                % "json-smart"           % V.jsonSmart

  val streamsCore = "com.snowplowanalytics" %% "streams-core"   % V.streams
  val kinesis     = "com.snowplowanalytics" %% "kinesis"        % V.streams
  val kafka       = "com.snowplowanalytics" %% "kafka"          % V.streams
  val pubsub      = "com.snowplowanalytics" %% "pubsub"         % V.streams
  val loaders     = "com.snowplowanalytics" %% "loaders-common" % V.streams
  val runtime     = "com.snowplowanalytics" %% "runtime-common" % V.streams

  // tests
  val specs2            = "org.specs2"    %% "specs2-core"                % V.specs2           % Test
  val catsEffectSpecs2  = "org.typelevel" %% "cats-effect-testing-specs2" % V.catsEffectSpecs2 % Test
  val catsEffectTestkit = "org.typelevel" %% "cats-effect-testkit"        % V.catsEffect       % Test

  val coreDependencies = Seq(
    streamsCore,
    loaders,
    runtime,
    http4sCirce,
    decline,
    sentry,
    snowflakeIngest,
    snowflakeJDBC,
    doobie,
    circeGenericExtra,
    jacksonCore,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Test
  )

  val kafkaDependencies = Seq(
    kafka,
    slf4j % Runtime,
    jaxb  % Runtime,
    azureIdentity,
    nettyCodecHttp,
    reactorNetty,
    snappyJava,
    nimbusJoseJwt,
    jsonSmart,
    specs2,
    catsEffectSpecs2
  )

  val pubsubDependencies = Seq(
    pubsub,
    protobufJava,
    protobufUtil,
    jaxb  % Runtime,
    slf4j % Runtime,
    specs2,
    catsEffectSpecs2
  )

  val kinesisDependencies = Seq(
    kinesis,
    protobufJava,
    jaxb    % Runtime,
    slf4j   % Runtime,
    stsSdk2 % Runtime,
    nettyCodecHttp,
    specs2,
    catsEffectSpecs2
  )

}
