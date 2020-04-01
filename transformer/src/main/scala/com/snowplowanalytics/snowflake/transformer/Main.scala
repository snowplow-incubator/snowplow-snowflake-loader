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
package com.snowplowanalytics.snowflake.transformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import cats.data.EitherT
import cats.syntax.functor._
import cats.syntax.either._
import cats.effect.{ IO, IOApp, ExitCode }

import io.circe.syntax._

import com.snowplowanalytics.iglu.core.{ SchemaKey, SchemaVer }

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._

import com.snowplowanalytics.snowflake.core.{ProcessManifest, Cli, idClock}
import com.snowplowanalytics.snowflake.transformer.TransformerJobConfig.S3Config


object Main extends IOApp {

  val AtomicSchema = SchemaKey("com.snowplowanalytics.snowplow", "atomic", "jsonschema", SchemaVer.Full(1, 0, 0))

  def run(args: List[String]): IO[ExitCode] = {
    Cli.Transformer.parse(args).value.flatMap {
      case Right(Cli.Transformer(appConfig, igluClient, inbatch, eventsManifestConfig, inputCompressionFormat)) =>

        // Always use EMR Role role for manifest-access
        for {
          state <- ProcessManifest.initState[IO](appConfig.awsRegion)
          manifest = ProcessManifest.awsSyncProcessManifest[IO](state)

          // Get run folders that are not in RunManifest in any form
          runFolders <- manifest.getUnprocessed(appConfig.manifest, appConfig.input)
          atomic <- EitherT(igluClient.resolver.lookupSchema(AtomicSchema))
            .leftMap(_.value.asJson.noSpaces)
            .flatMap(json => Schema.parse(json).toRight("Atomic event schema was invalid").toEitherT)
            .value
            .flatMap {
              case Right(schema) => IO.pure(schema)
              case Left(error) => IO.raiseError[Schema](new RuntimeException(error))
            }

          // Eager SparkContext initializing to avoid YARN timeout
          spark <- getSession
          exitCode <- runFolders match {
            case Right(folders) =>
              val configs = folders.map(S3Config(appConfig.input, appConfig.stageUrl, appConfig.badOutputUrl, _))
              TransformerJob.run(spark, manifest, appConfig.manifest, configs, eventsManifestConfig, inbatch, atomic,
                inputCompressionFormat
              ).as(ExitCode.Success)
            case Left(error) =>
              die(s"Cannot get list of unprocessed folders\n$error")
          }
        } yield exitCode
      case Left(error) => die(error)
    }
  }

  def getSession: IO[SparkSession] =
    IO {
      val config = new SparkConf()
        .setAppName("snowflake-transformer")
        .setIfMissing("spark.master", "local[*]")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .registerKryoClasses(TransformerJob.classesToRegister)

      SparkSession.builder().config(config).getOrCreate()
    }

  private def die(message: String): IO[ExitCode] =
    IO(System.err.println(message)).as(ExitCode.Error)
}
