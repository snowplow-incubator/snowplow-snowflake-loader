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
package com.snowplowanalytics.snowflake.core

import java.util.{ Base64, UUID }

import org.specs2.mutable.Specification

import cats.effect.IO

import io.circe.Json

import com.snowplowanalytics.iglu.client.{ Client, Resolver }
import com.snowplowanalytics.iglu.client.validator.CirceValidator
import com.snowplowanalytics.iglu.client.resolver.registries.Registry

import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig
import com.snowplowanalytics.snowflake.core.Config.SetupSteps
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}
import com.snowplowanalytics.snowflake.core.Cli.CompressionFormat


class ConfigSpec extends Specification {
  "Parse valid setup configuration" in e1
  "Parse valid load configuration" in e2
  "Parse valid base64-encoded configuration" in e3
  "Parse valid S3 without trailing slash" in e4
  "Parse valid S3 with trailing slash and s3n scheme" in e5
  "Fail to parse invalid scheme" in e6
  "Parse valid base64-encoded configuration with roleArn" in e7
  "Parse valid load configuration with EC2-stored password and Role ARN" in e8
  "Parse valid load without credentials" in e9
  "Parse valid base64-encoded events manifest configuration" in e10
  "Parse valid configuration with optional params" in e11
  "Parse valid configuration with set setup steps" in e12
  "Fail to parse configuration with bad setup steps" in e13
  "Parse valid configuration without bad output url" in e14

  val configUrl = getClass.getResource("/valid-config.json")
  val resolverUrl = getClass.getResource("/resolver.json")

  val resolverConfig =
    """
      |{
      |   "schema":"iglu:com.snowplowanalytics.iglu/resolver-config/jsonschema/1-0-0",
      |   "data":{
      |      "cacheSize":500,
      |      "repositories":[
      |         {
      |            "name":"Iglu Central",
      |            "priority":10,
      |            "vendorPrefixes":[
      |               "com.snowplowanalytics"
      |            ],
      |            "connection":{
      |               "http":{
      |                  "uri":"http://iglucentral.com"
      |               }
      |            }
      |         }
      |      ]
      |   }
      |}
      |
    """.stripMargin

  val resolverBase64 = encodeToBase64(resolverConfig)

  val roleConfigUrl = getClass.getResource("/valid-config-role.json")
  val secureConfigUrl = getClass.getResource("/valid-config-secure.json")
  val noauthConfigUrl = getClass.getResource("/valid-config-noauth.json")
  val optionalParamsConfigUrl = getClass.getResource("/valid-config-optional.json")

  def encodeToBase64(str: String): String = Base64.getEncoder.encodeToString(str.getBytes)

  def e1 = {
    val args = List(
      "setup",

      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${configUrl.getPath}"
    ).toArray

    val expected = Cli.Loader.Setup(
      Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "ABCD",
          secretAccessKey = "abcd"
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        schema = "atomic",
        maxError = None,
        jdbcHost = None),
      Set(),
      false)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e2 = {
    val args = List(
      "load",

      "--dry-run",
      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${configUrl.getPath}").toArray

    val expected = Cli.Loader.Load(
      Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "ABCD",
          secretAccessKey = "abcd"
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        input = s3("s3://snowflake/input/"),
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      true)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e3 = {
    val config =
      s"""
        |{
        |   "schema": "${Config.ConfigSchema.toSchemaUri}",
        |   "data":{
        |      "name":"Snowflake base64",
        |      "auth":{
        |         "accessKeyId":"ABCDA",
        |         "secretAccessKey":"abcd"
        |      },
        |      "awsRegion":"us-east-1",
        |      "manifest":"snowflake-manifest",
        |      "snowflakeRegion":"us-west-1",
        |      "database":"test_db",
        |      "input":"s3://snowflake/input/",
        |      "stage":"some_stage",
        |      "stageUrl":"s3://snowflake/output/",
        |      "badOutputUrl": "s3://badRows/output/",
        |      "warehouse":"snowplow_wh",
        |      "schema":"atomic",
        |      "account":"snowplow",
        |      "username":"anton",
        |      "password":"Supersecret2",
        |      "purpose":"ENRICHED_EVENTS"
        |   }
        |}
      """.stripMargin

    val args = List(
      "load",

      "--dry-run",
      "--base64",
      "--resolver", resolverBase64,
      "--config", encodeToBase64(config)
    ).toArray

    val expected = Cli.Loader.Load(
      Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "ABCDA",
          secretAccessKey = "abcd"
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      true)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)

  }

  def e4 = {
    val result = Config.S3Folder.parse("s3://cross-batch-test/archive/some-folder")
    result must beRight(s3("s3://cross-batch-test/archive/some-folder/"))
  }

  def e5 = {
    val result = Config.S3Folder.parse("s3n://cross-batch-test/archive/some-folder/")
    result must beRight(s3("s3://cross-batch-test/archive/some-folder/"))
  }

  def e6 = {
    val result = Config.S3Folder.parse("http://cross-batch-test/archive/some-folder/")
    result must beLeft("Bucket name [http://cross-batch-test/archive/some-folder/] must start with s3:// prefix")
  }

  def e7 = {
    val config =
      s"""
        |{
        |   "schema": "${Config.ConfigSchema.toSchemaUri}",
        |   "data":{
        |      "name":"Snowflake",
        |      "auth":{
        |         "roleArn":"arn:aws:iam::719197435995:role/SnowflakeRole",
        |         "sessionDuration":900
        |      },
        |      "awsRegion":"us-east-1",
        |      "manifest":"snowflake-manifest",
        |      "snowflakeRegion":"us-west-1",
        |      "database":"test_db",
        |      "input":"s3://snowflake/input/",
        |      "stage":"some_stage",
        |      "stageUrl":"s3://snowflake/output/",
        |      "badOutputUrl": "s3://badRows/output/",
        |      "warehouse":"snowplow_wh",
        |      "schema":"atomic",
        |      "account":"snowplow",
        |      "username":"anton",
        |      "password":"Supersecret2",
        |      "purpose":"ENRICHED_EVENTS"
        |   }
        |}
      """.stripMargin

    val args = List(
      "setup",

      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--base64"
    ).toArray

    val expected = Cli.Loader.Setup(
      Config(
        auth = Config.AuthMethod.RoleAuth(
          roleArn = "arn:aws:iam::719197435995:role/SnowflakeRole",
          sessionDuration = 900
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      Set(),
      false)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e8 = {
    val args = List(
      "load",

      "--dry-run",
      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${secureConfigUrl.getPath}").toArray

    val expected = Cli.Loader.Load(
      Config(
        auth = Config.AuthMethod.RoleAuth(
          roleArn = "arn:aws:iam::111222333444:role/SnowflakeLoadRole",
          sessionDuration = 900
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        input = s3("s3://snowflake/input/"),
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.EncryptedKey(
          Config.EncryptedConfig(
            Config.ParameterStoreConfig("snowplow.snowflakeloader.snowflake.password"))),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      true)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e9 = {
    val args = List(
      "load",

      "--dry-run",
      "--resolver", s"${resolverUrl.getPath}",
      "--config", s"${noauthConfigUrl.getPath}").toArray

    val expected = Cli.Loader.Load(
      Config(
        auth = Config.AuthMethod.StageAuth,
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        input = s3("s3://snowflake/input/"),
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.EncryptedKey(
          Config.EncryptedConfig(
            Config.ParameterStoreConfig("snowplow.snowflakeloader.snowflake.password"))),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      true)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e10 = {
    val config =
      s"""
        |{
        |   "schema":"${Config.ConfigSchema.toSchemaUri}",
        |   "data":{
        |      "name":"Snowflake base64",
        |      "auth":{
        |         "accessKeyId":"ABCD",
        |         "secretAccessKey":"abcd"
        |      },
        |      "awsRegion":"us-east-1",
        |      "manifest":"snowflake-manifest",
        |      "snowflakeRegion":"us-west-1",
        |      "database":"test_db",
        |      "input":"s3://snowflake/input/",
        |      "stage":"some_stage",
        |      "stageUrl":"s3://snowflake/output/",
        |      "badOutputUrl": "s3://badRows/output/",
        |      "warehouse":"snowplow_wh",
        |      "schema":"atomic",
        |      "account":"snowplow",
        |      "username":"anton",
        |      "password":"Supersecret2",
        |      "purpose":"ENRICHED_EVENTS"
        |   }
        |}
      """.stripMargin

    val eventManifestConfig =
      s"""
        |{
        |   "schema":"iglu:com.snowplowanalytics.snowplow.storage/amazon_dynamodb_config/jsonschema/2-0-0",
        |   "data":{
        |      "name":"local",
        |      "auth":{
        |         "accessKeyId":"fakeAccessKeyId",
        |         "secretAccessKey":"fakeSecretAccessKey"
        |      },
        |      "awsRegion":"us-west-1",
        |      "dynamodbTable":"snowplow-integration-test-crossbatch-dedupe",
        |      "id":"56799a26-980c-4148-8bd9-c021b988c669",
        |      "purpose":"EVENTS_MANIFEST"
        |   }
        |}
      """.stripMargin

    val args = List(
      "--inbatch-deduplication",
      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--events-manifest", encodeToBase64(eventManifestConfig),
      "--input-compression-format", "gzip"
    ).toArray

    val expected = Cli.Transformer(
      Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "ABCD",
          secretAccessKey = "abcd"
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      Client[IO, Json](Resolver(List(Registry.IgluCentral), None), CirceValidator),
      true,
      Some(EventsManifestConfig.DynamoDb(
        id = Some(UUID.fromString("56799a26-980c-4148-8bd9-c021b988c669")),
        name = "local",
        auth = Some(EventsManifestConfig.DynamoDb.Credentials(
          accessKeyId = "fakeAccessKeyId",
          secretAccessKey = "fakeSecretAccessKey")
        ),
        awsRegion = "us-west-1",
        dynamodbTable = "snowplow-integration-test-crossbatch-dedupe"
      )),
      Some(CompressionFormat.Gzip)
    )

    Cli.Transformer.parse(args).value.unsafeRunSync() must beRight.like {
      case transformer @ Cli.Transformer(_, client, _, _, _) =>
        val updatedClient: Resolver[IO] = client.resolver.copy(cache = None)
        val updatedConfig = transformer.copy(igluClient = transformer.igluClient.copy(resolver = updatedClient))
        updatedConfig must beEqualTo(expected)
    }
  }

  def e11 = {
    val config =
      s"""
         |{
         |   "schema":"${Config.ConfigSchema.toSchemaUri}",
         |   "data":{
         |      "name":"Snowflake",
         |      "auth":{
         |         "accessKeyId":"ABCD",
         |         "secretAccessKey":"abcd"
         |      },
         |      "awsRegion":"us-east-1",
         |      "manifest":"snowflake-manifest",
         |      "snowflakeRegion":"us-west-1",
         |      "database":"test_db",
         |      "input":"s3://snowflake/input/",
         |      "stage":"some_stage",
         |      "stageUrl":"s3://snowflake/output/",
         |      "badOutputUrl": "s3://badRows/output/",
         |      "warehouse":"snowplow_wh",
         |      "schema":"atomic",
         |      "account":"snowplow",
         |      "username":"anton",
         |      "password":"Supersecret2",
         |      "purpose":"ENRICHED_EVENTS",
         |      "maxError":10000,
         |      "jdbcHost":"snowplow.us-west-1.azure.snowflakecomputing.com"
         |   }
         |}
       """.stripMargin

    val args = List(
      "load",

      "--dry-run",
      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--base64"
    ).toArray

    val expected = Cli.Loader.Load(
      Config(
        auth = Config.AuthMethod.CredentialsAuth(
          accessKeyId = "ABCD",
          secretAccessKey = "abcd"
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = Some(10000),
        jdbcHost = Some("snowplow.us-west-1.azure.snowflakecomputing.com")),
      true)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e12 = {
    val config =
      s"""
         |{
         |   "schema":"${Config.ConfigSchema.toSchemaUri}",
         |   "data":{
         |      "name":"Snowflake",
         |      "auth":{
         |         "roleArn":"arn:aws:iam::719197435995:role/SnowflakeRole",
         |         "sessionDuration":900
         |      },
         |      "awsRegion":"us-east-1",
         |      "manifest":"snowflake-manifest",
         |      "snowflakeRegion":"us-west-1",
         |      "database":"test_db",
         |      "input":"s3://snowflake/input/",
         |      "stage":"some_stage",
         |      "stageUrl":"s3://snowflake/output/",
         |      "badOutputUrl": "s3://badRows/output/",
         |      "warehouse":"snowplow_wh",
         |      "schema":"atomic",
         |      "account":"snowplow",
         |      "username":"anton",
         |      "password":"Supersecret2",
         |      "purpose":"ENRICHED_EVENTS"
         |   }
         |}
       """.stripMargin

    val args = List(
      "setup",

      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--base64",
      "--skip", "schema",
      "--skip", "stage",
      "--skip", "table"
    ).toArray

    val expected = Cli.Loader.Setup(
      Config(
        auth = Config.AuthMethod.RoleAuth(
          roleArn = "arn:aws:iam::719197435995:role/SnowflakeRole",
          sessionDuration = 900
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = Some(s3("s3://badRows/output/")),
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      Set(SetupSteps.Schema, SetupSteps.Stage, SetupSteps.Table),
      false)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }

  def e13 = {
    val config =
      s"""
         |{
         |   "schema":"${Config.ConfigSchema.toSchemaUri}",
         |   "data":{
         |      "name":"Snowflake",
         |      "auth":{
         |         "roleArn":"arn:aws:iam::719197435995:role/SnowflakeRole",
         |         "sessionDuration":900
         |      },
         |      "awsRegion":"us-east-1",
         |      "manifest":"snowflake-manifest",
         |      "snowflakeRegion":"us-west-1",
         |      "database":"test_db",
         |      "input":"s3://snowflake/input/",
         |      "stage":"some_stage",
         |      "stageUrl":"s3://snowflake/output/",
         |      "badOutputUrl": "s3://badRows/output/",
         |      "warehouse":"snowplow_wh",
         |      "schema":"atomic",
         |      "account":"snowplow",
         |      "username":"anton",
         |      "password":"Supersecret2",
         |      "purpose":"ENRICHED_EVENTS"
         |   }
         |}
       """.stripMargin

    val args = List(
      "setup",

      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--base64",
      "--skip", "schema",
      "--skip", "stage",
      "--skip", "foo",
      "--skip", "bar"
    ).toArray

    val result = Cli.Loader.parse(args).value.unsafeRunSync()

    result must beLeft.like {
      case help => help must contain("Step bar is unknown. Available options: schema, table, warehouse, fileformat, stage")
    }
  }

  def e14 = {
    val config =
      s"""
         |{
         |   "schema":"${Config.ConfigSchema.toSchemaUri}",
         |   "data":{
         |      "name":"Snowflake",
         |      "auth":{
         |         "roleArn":"arn:aws:iam::719197435995:role/SnowflakeRole",
         |         "sessionDuration":900
         |      },
         |      "awsRegion":"us-east-1",
         |      "manifest":"snowflake-manifest",
         |      "snowflakeRegion":"us-west-1",
         |      "database":"test_db",
         |      "input":"s3://snowflake/input/",
         |      "stage":"some_stage",
         |      "stageUrl":"s3://snowflake/output/",
         |      "warehouse":"snowplow_wh",
         |      "schema":"atomic",
         |      "account":"snowplow",
         |      "username":"anton",
         |      "password":"Supersecret2",
         |      "purpose":"ENRICHED_EVENTS"
         |   }
         |}
       """.stripMargin

    val args = List(
      "setup",

      "--resolver", resolverBase64,
      "--config", encodeToBase64(config),
      "--base64"
    ).toArray

    val expected = Cli.Loader.Setup(
      Config(
        auth = Config.AuthMethod.RoleAuth(
          roleArn = "arn:aws:iam::719197435995:role/SnowflakeRole",
          sessionDuration = 900
        ),
        awsRegion = "us-east-1",
        manifest = "snowflake-manifest",
        stage = "some_stage",
        stageUrl = s3("s3://snowflake/output/"),
        badOutputUrl = None,
        snowflakeRegion = "us-west-1",
        schema = "atomic",
        username = "anton",
        password = Config.PasswordConfig.PlainText("Supersecret2"),
        input = s3("s3://snowflake/input/"),
        account = "snowplow",
        warehouse = "snowplow_wh",
        database = "test_db",
        maxError = None,
        jdbcHost = None),
      Set(),
      false)

    Cli.Loader.parse(args).value.unsafeRunSync() must beRight(expected)
  }
}
