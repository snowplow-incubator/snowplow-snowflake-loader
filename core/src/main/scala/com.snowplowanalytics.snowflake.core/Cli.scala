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

import java.nio.file.{Files, InvalidPathException, Path, Paths}
import java.util.Base64

import cats.data.{EitherT, ValidatedNel}
import cats.effect.IO
import cats.implicits._
import io.circe.Json
import io.circe.syntax._
import io.circe.parser.{parse => jsonParse}
import com.monovore.decline.{Argument, Command, Opts}
import com.monovore.decline.enumeratum._
import com.snowplowanalytics.iglu.client.Client
import com.snowplowanalytics.iglu.core.SelfDescribingData
import com.snowplowanalytics.iglu.core.circe.implicits._
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig
import enumeratum._

import scala.collection.immutable

object Cli {
  import Config._

  /** Config files for Loader can be passed either as FS path
    * or as base64-encoded JSON (if `--base64` is provided) */
  type PathOrJson = Either[Path, Json]

  object PathOrJson {
    def parse(string: String, encoded: Boolean): ValidatedNel[String, PathOrJson] = {
      val result = if (encoded)
        Either
          .catchOnly[IllegalArgumentException](new String(Base64.getDecoder.decode(string)))
          .leftMap(_.getMessage)
          .flatMap(s => jsonParse(s).leftMap(_.show))
          .map(_.asRight)
      else Either.catchOnly[InvalidPathException](Paths.get(string).asLeft).leftMap(_.getMessage)
      result
        .leftMap(error => s"Cannot parse as ${if (encoded) "base64-encoded JSON" else "FS path"}: $error")
        .toValidatedNel
    }

    def load(value: PathOrJson): EitherT[IO, String, Json] =
      value match {
        case Right(json) =>
          EitherT.rightT[IO, String](json)
        case Left(path) =>
          Either
            .catchNonFatal(new String(Files.readAllBytes(path)))
            .leftMap(e => s"Cannot read the path: ${e.getMessage}")
            .flatMap(s => jsonParse(s).leftMap(_.show))
            .toEitherT[IO]
      }
  }

  /** Base64-encoded JSON */
  case class Base64Encoded(json: Json) extends AnyVal

  object Base64Encoded {
    def parse(string: String): Either[String, Base64Encoded] =
      Either
        .catchOnly[IllegalArgumentException](Base64.getDecoder.decode(string))
        .map(bytes => new String(bytes))
        .flatMap(str => jsonParse(str))
        .leftMap(e => s"Cannot parse ${string} as Base64-encoded JSON: ${e.getMessage}")
        .map(json => Base64Encoded(json))
  }

  implicit def base64EncodedDeclineArg: Argument[Base64Encoded] =
    new Argument[Base64Encoded] {
      def read(string:  String): ValidatedNel[String, Base64Encoded] =
        Base64Encoded.parse(string).toValidatedNel

      def defaultMetavar: String = "base64"
    }

  sealed trait CompressionFormat extends EnumEntry with EnumEntry.Lowercase
  object CompressionFormat extends Enum[CompressionFormat] {
    case object Gzip extends CompressionFormat
    case object LZO extends CompressionFormat
    case object Snappy extends CompressionFormat
    case object None extends CompressionFormat

    val values: immutable.IndexedSeq[CompressionFormat] = findValues
  }

  case class Transformer(loaderConfig: Config,
                         igluClient: Client[IO, Json],
                         inbatch: Boolean,
                         eventsManifestConfig: Option[EventsManifestConfig],
                         inputCompressionFormat: Option[CompressionFormat])

  object Transformer {
    case class Raw(loaderConfig: Base64Encoded,
                   resolver: Base64Encoded,
                   inbatch: Boolean,
                   eventsManifestConfig: Option[Base64Encoded],
                   inputCompressionFormat: Option[CompressionFormat])

    def parse(args: Seq[String]): EitherT[IO, String, Transformer] =
      transformer
        .parse(args)
        .leftMap(_.toString).toEitherT[IO]
        .flatMap(init)

    def init(raw: Raw): EitherT[IO, String, Transformer] =
      for {
        igluClient <- Client.parseDefault[IO](raw.resolver.json).leftMap(_.show)
        configData <- SelfDescribingData.parse(raw.loaderConfig.json).leftMap(e => s"Configuration JSON is not self-describing, ${e.code}").toEitherT[IO]
        _ <- igluClient.check(configData).leftMap(e => s"Iglu validation failed with following error\n: ${e.asJson.spaces2}")
        cfg <- configData.data.as[Config].toEitherT[IO].leftMap(e => s"Error while decoding configuration JSON, ${e.show}")
        manifest <- raw.eventsManifestConfig match {
          case Some(json) => EventsManifestConfig.parseJson[IO](igluClient, json.json).map(_.some)
          case None => EitherT.rightT[IO, String](none[EventsManifestConfig])
        }
      } yield Transformer(cfg, igluClient, raw.inbatch, manifest, raw.inputCompressionFormat)

  }

  sealed trait Loader extends Product with Serializable
  object Loader {

    sealed trait RawCli extends Product with Serializable {
      def loaderConfig: PathOrJson
      def resolver: PathOrJson
    }

    final case class LoadRaw(loaderConfig: PathOrJson, resolver: PathOrJson, dryRun: Boolean) extends RawCli
    final case class SetupRaw(loaderConfig: PathOrJson, resolver: PathOrJson, skip: Set[SetupSteps], dryRun: Boolean) extends RawCli
    final case class MigrateRaw(loaderConfig: PathOrJson, resolver: PathOrJson, loaderVersion: String, dryRun: Boolean) extends RawCli

    final case class Load(loaderConfig: Config, dryRun: Boolean) extends Loader
    final case class Setup(loaderConfig: Config, skip: Set[SetupSteps], dryRun: Boolean) extends Loader
    final case class Migrate(loaderConfig: Config, loaderVersion: String, dryRun: Boolean) extends Loader

    def parse(args: Seq[String]) =
      loader
        .parse(args)
        .leftMap(_.toString).toEitherT[IO]
        .flatMap(init)

    def init(rawCli: RawCli): EitherT[IO, String, Loader] =
      for {
        resolverJson <- PathOrJson.load(rawCli.resolver)
        igluClient <- Client.parseDefault[IO](resolverJson).leftMap(_.show)
        configJson <- PathOrJson.load(rawCli.loaderConfig)
        configData <- SelfDescribingData.parse(configJson).leftMap(e => s"Configuration JSON is not self-describing, ${e.code}").toEitherT[IO]
        _ <- igluClient.check(configData).leftMap(e => s"Iglu validation failed with following error\n: ${e.asJson.spaces2}")
        cfg <- configData.data.as[Config].toEitherT[IO].leftMap(e => s"Error while decoding configuration JSON, ${e.show}")
      } yield rawCli match {
        case LoadRaw(_, _, dryRun) => Load(cfg, dryRun)
        case SetupRaw(_, _, skip, dryRun) => Setup(cfg, skip, dryRun)
        case MigrateRaw(_, _, version, dryRun) => Migrate(cfg, version, dryRun)
      }
  }

  implicit def stepDeclineArg = new Argument[SetupSteps] {
    def read(string: String): ValidatedNel[String, SetupSteps] =
      SetupSteps
        .withNameInsensitiveOption(string)
        .toValidNel(s"Step $string is unknown. Available options: ${SetupSteps.allStrings}")

    override def defaultMetavar: String = "step"
  }

  val dryRun = Opts.flag("dry-run", "Do not perform database actions, only print statements to stdout").orFalse
  val base64 = Opts.flag("base64", "Configuration passed as Base64-encoded string, not as file path").orFalse
  val version = Opts.option[String]("loader-version", s"Snowplow Snowflake Loader version to make the table compatible with")
  val steps = Opts.options[SetupSteps]("skip", s"Skip the setup step. Available steps: ${Config.SetupSteps.values}").orNone.map(_.toList.unite.toSet)

  val resolver = Opts.option[String]("resolver", "Iglu Resolver JSON config, FS path or base64-encoded")
  val resolverEncoded = Opts.option[Base64Encoded]("resolver", "Iglu Resolver JSON config, base64-encoded")

  val config = Opts.option[String]("config", "Snowflake Loader JSON config, FS path or base64-encoded")
  val configEncoded = Opts.option[Base64Encoded]("config", "Snowflake Loader JSON config, base64-encoded")

  val setupOpt = (config, base64, steps, dryRun, resolver).tupled.mapValidated {
    case (cfg, encoded, steps, dry, res) =>
      (PathOrJson.parse(cfg, encoded), PathOrJson.parse(res, encoded)).mapN { (c, r) => Loader.SetupRaw(c, r, steps, dry) }
  }
  val setup = Opts.subcommand(Command("setup", "Perform setup actions", true)(setupOpt))

  val migrateOpt = (config, base64, version, dryRun, resolver).tupled.mapValidated {
    case (cfg, encoded, version, dry, res) =>
      (PathOrJson.parse(cfg, encoded), PathOrJson.parse(res, encoded)).mapN { (c, r) => Loader.MigrateRaw(c, r, version, dry) }
  }
  val migrate = Opts.subcommand(Command("migrate", "Load data into a warehouse", true)(migrateOpt))

  val loadOpt = (config, base64, dryRun, resolver).tupled.mapValidated {
    case (cfg, encoded, dry, res) =>
      (PathOrJson.parse(cfg, encoded), PathOrJson.parse(res, encoded)).mapN { (c, r) => Loader.LoadRaw(c, r, dry) }
  }
  val load = Opts.subcommand(Command("load", "Load data into a warehouse", true)(loadOpt))

  val inBatchDedupe = Opts.flag("inbatch-deduplication", "Enable in-batch natural deduplication").orFalse
  val evantsManifest = Opts.option[Base64Encoded]("events-manifest", "Snowplow Events Manifest JSON config, to enable cross-batch deduplication, base64-encoded").orNone
  val inputCompressionFormat = Opts.option[CompressionFormat](
    "input-compression-format",
    "The compression used by input files of the Spark job. Supported values: none, gzip, lzo, snappy."
  ).orNone

  val loader = Command("snowplow-snowflake-loader", "Snowplow Database orchestrator")(load.orElse(setup).orElse(migrate))

  val transformer = Command("snowplow-snowflake-transformer", "Spark job to transform enriched data to Snowflake-compatible format") {
    (configEncoded, resolverEncoded, inBatchDedupe, evantsManifest, inputCompressionFormat).mapN(Transformer.Raw)
  }
}

