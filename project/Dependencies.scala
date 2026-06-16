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
    val slf4j         = "2.0.17"
    val azureSdk      = "1.18.3"
    val sentry        = "6.25.2"
    val snowflake     = "4.4.3-unshaded"
    val snowflakeJdbc = "3.25.1"
    val jaxb          = "2.3.1"
    val awsSdk2       = "2.44.4"
    val netty         = "4.1.133.Final" // Version override
    val jsonSmart     = "2.5.2" // Version override
    val commonsLang   = "3.18.0" // Version override
    val kafkaClient   = "3.9.2" // Version override
    val jackson       = "2.18.7" // Fix CVE
    val bouncycastle  = "1.84" // Fix CVE
    val grpcNetty     = "1.81.0" // Fix CVE
    val lz4Java       = "1.8.1" // Fix CVE
    val aircompressor = "2.0.3" // Fix CVE

    // Snowplow
    val streams = "0.24.1"

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
  val slf4j         = "org.slf4j" % "slf4j-simple"   % V.slf4j
  val azureIdentity = "com.azure" % "azure-identity" % V.azureSdk
  val sentry        = "io.sentry" % "sentry"         % V.sentry
  val snowflakeIngest =
    ("net.snowflake" % "snowflake-ingest-sdk" % V.snowflake).excludeAll(ExclusionRule(organization = "org.apache.iceberg"))
  val snowflakeJdbc     = "net.snowflake"              % "snowflake-jdbc-thin" % V.snowflakeJdbc
  val jaxb              = "javax.xml.bind"             % "jaxb-api"            % V.jaxb
  val stsSdk2           = "software.amazon.awssdk"     % "sts"                 % V.awsSdk2
  val nettyCodec        = "io.netty"                   % "netty-codec"         % V.netty
  val nettyCodecHttp    = "io.netty"                   % "netty-codec-http"    % V.netty
  val nettyCodecHttp2   = "io.netty"                   % "netty-codec-http2"   % V.netty
  val nettyHandlerProxy = "io.netty"                   % "netty-handler-proxy" % V.netty
  val nettyCodecDns     = "io.netty"                   % "netty-codec-dns"     % V.netty
  val jsonSmart         = "net.minidev"                % "json-smart"          % V.jsonSmart
  val commonsLang       = "org.apache.commons"         % "commons-lang3"       % V.commonsLang
  val kafkaClient       = "org.apache.kafka"           % "kafka-clients"       % V.kafkaClient
  val jacksonCore       = "com.fasterxml.jackson.core" % "jackson-core"        % V.jackson
  val bcpkix            = "org.bouncycastle"           % "bcpkix-jdk18on"      % V.bouncycastle
  val bcprov            = "org.bouncycastle"           % "bcprov-jdk18on"      % V.bouncycastle
  val bcutil            = "org.bouncycastle"           % "bcutil-jdk18on"      % V.bouncycastle
  val grpcNettyShaded   = "io.grpc"                    % "grpc-netty-shaded"   % V.grpcNetty
  val lz4Java           = "org.lz4"                    % "lz4-java"            % V.lz4Java
  val aircompressor     = "io.airlift"                 % "aircompressor"       % V.aircompressor

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
    snowflakeIngest,
    snowflakeJdbc,
    doobie,
    circeGenericExtra,
    commonsLang,
    nettyCodec,
    nettyCodecHttp,
    nettyCodecHttp2,
    nettyHandlerProxy,
    jacksonCore,
    bcpkix,
    bcprov,
    bcutil,
    grpcNettyShaded,
    aircompressor,
    specs2,
    catsEffectSpecs2,
    catsEffectTestkit,
    slf4j % Test
  )

  val kafkaDependencies = Seq(
    kafka,
    kafkaClient,
    slf4j % Runtime,
    jaxb  % Runtime,
    azureIdentity,
    jsonSmart,
    nettyCodecDns,
    lz4Java,
    specs2,
    catsEffectSpecs2
  )

  val pubsubDependencies = Seq(
    pubsub,
    jaxb  % Runtime,
    slf4j % Runtime,
    specs2,
    catsEffectSpecs2
  )

  val kinesisDependencies = Seq(
    kinesis,
    jaxb    % Runtime,
    slf4j   % Runtime,
    stsSdk2 % Runtime,
    specs2,
    catsEffectSpecs2
  )

}
