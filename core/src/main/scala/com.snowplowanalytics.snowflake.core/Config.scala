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

import cats.implicits._

import io.circe.{Decoder, DecodingFailure}
import io.circe.generic.semiauto._

import enumeratum._

import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer}

/** Common loader configuration interface, extracted from configuration file */
case class Config(
  auth: Config.AuthMethod,
  awsRegion: String,
  manifest: String,

  snowflakeRegion: String,
  stage: String,
  stageUrl: Config.S3Folder,
  badOutputUrl: Option[Config.S3Folder],
  input: Config.S3Folder,
  username: String,
  password: Config.PasswordConfig,
  account: String,
  warehouse: String,
  database: String,
  schema: String,
  maxError: Option[Int],
  jdbcHost: Option[String]
)

object Config {

  val ConfigSchema = SchemaKey("com.snowplowanalytics.snowplow.storage", "snowflake_config", "jsonschema", SchemaVer.Full(1, 0, 2))

  implicit val configDecoder: Decoder[Config] =
    deriveDecoder[Config]

  sealed trait SetupSteps extends EnumEntry

  object SetupSteps extends Enum[SetupSteps] {
    val values = findValues

    val allStrings = values.map(_.toString.toLowerCase).mkString(", ")

    case object Schema extends SetupSteps
    case object Table extends SetupSteps
    case object Warehouse extends SetupSteps
    case object FileFormat extends SetupSteps
    case object Stage extends SetupSteps
  }

  /** Available methods to authenticate Snowflake loading */
  sealed trait AuthMethod extends Product with Serializable
  object AuthMethod {
    final case class RoleAuth(roleArn: String, sessionDuration: Int) extends AuthMethod
    final case class CredentialsAuth(accessKeyId: String, secretAccessKey: String) extends AuthMethod
    final case object StageAuth extends AuthMethod
    final case class StorageIntegration(integrationName: String) extends AuthMethod

    implicit val authMethodCirceDecoder: Decoder[AuthMethod] =
      List[Decoder[AuthMethod]](
        deriveDecoder[RoleAuth].widen,
        deriveDecoder[CredentialsAuth].widen,
        deriveDecoder[StorageIntegration].widen,
        Decoder.const(StageAuth).widen
      ).reduce(_ or _)
  }

  /** Reference to encrypted entity inside EC2 Parameter Store */
  final case class ParameterStoreConfig(parameterName: String)

  implicit val circeJsonParameterStoreConfig: Decoder[ParameterStoreConfig] =
    deriveDecoder[ParameterStoreConfig]


  /** Reference to encrypted key (EC2 Parameter Store only so far) */
  final case class EncryptedConfig(ec2ParameterStore: ParameterStoreConfig)

  implicit val circeJsonEncryptedConfigDecoder: Decoder[EncryptedConfig] =
    deriveDecoder[EncryptedConfig]


  sealed trait PasswordConfig {
    def getUnencrypted: String = this match {
      case PasswordConfig.PlainText(plain) => plain
      case PasswordConfig.EncryptedKey(EncryptedConfig(key)) => key.parameterName
    }
  }

  object PasswordConfig {
    final case class PlainText(value: String) extends PasswordConfig
    final case class EncryptedKey(value: EncryptedConfig) extends PasswordConfig

    private val plainTextDecoder: Decoder[PlainText] =
      Decoder[String].map(PlainText.apply)
    private val encryptedKeyDecoder: Decoder[EncryptedKey] =
      Decoder[EncryptedConfig].map(EncryptedKey.apply)

    implicit val circeJsonPasswordConfigDecoder: Decoder[PasswordConfig] =
      List[Decoder[PasswordConfig]](
        plainTextDecoder.widen,
        encryptedKeyDecoder.widen
      ).reduce(_ or _)
  }

  /**
    * Extract `s3://path/run=YYYY-MM-dd-HH-mm-ss/atomic-events` part from
    * Set of prefixes that can be used in config.yml
    * In the end it won't affect how S3 is accessed
    */
  val supportedPrefixes = Set("s3", "s3n", "s3a")

  /** Weak newtype replacement to mark string prefixed with s3:// and ended with trailing slash */
  object S3Folder {
    def parse(s: String): Either[String, S3Folder] = s match {
      case _ if !correctlyPrefixed(s) => Left(s"Bucket name [$s] must start with s3:// prefix")
      case _ if s.length > 1024       => Left("Key length cannot be more than 1024 symbols")
      case _                          => Right(appendTrailingSlash(fixPrefix(s)))
    }

    def coerce(s: String): Config.S3Folder = parse(s) match {
      case Right(f) => f
      case Left(error) => throw new IllegalArgumentException(error)
    }
  }

  case class S3Folder(path: String) extends AnyVal {
    /** Split valid S3 folder path to bucket and path */
    def splitS3Folder: (String, String) =
      stripS3Prefix(path).split("/").toList match {
        case head :: Nil => (head, "/")
        case head :: tail => (head, tail.mkString("/") + "/")
        case Nil => throw new IllegalArgumentException(s"Invalid S3 bucket path was passed") // Impossible
      }

    def isSubdirOf(other: S3Folder): Boolean =
      stripS3Prefix(path).startsWith(stripS3Prefix(other.toString))

    override def toString: String = path
  }

  implicit val s3folderCirceDecoder: Decoder[S3Folder] =
    Decoder.instance { str =>
      str.as[String].flatMap(s => S3Folder.parse(s).leftMap(e => DecodingFailure(e, str.history)))
    }

  private def correctlyPrefixed(s: String): Boolean =
    supportedPrefixes.foldLeft(false) { (result, prefix) =>
      result || s.startsWith(s"$prefix://")
    }

  private[core] def fixPrefix(s: String): String =
    if (s.startsWith("s3n")) "s3" + s.stripPrefix("s3n")
    else if (s.startsWith("s3a")) "s3" + s.stripPrefix("s3a")
    else s

  private def appendTrailingSlash(s: String): S3Folder =
    if (s.endsWith("/")) new S3Folder(s)
    else new S3Folder(s + "/")

  private def stripS3Prefix(s: String): String =
    s.stripPrefix("s3://")
}
