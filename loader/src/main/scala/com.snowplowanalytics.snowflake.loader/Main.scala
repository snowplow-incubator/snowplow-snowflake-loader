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
package com.snowplowanalytics.snowflake.loader

import cats.instances.all._

import cats.effect.{ExitCode, IO, IOApp, Sync}

import com.snowplowanalytics.snowflake.core.{Cli, ProcessManifest}
import com.snowplowanalytics.snowflake.loader.connection.Database

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    Cli.Loader.parse(args).value.flatMap {
      case Right(Cli.Loader.Load(config, dryRun, debug, appName)) =>
        implicit val logger: Logger[IO] = Logger.initLogger[IO](debug)
        val database = Database.init(dryRun)
        for {
          _ <- Logger[IO].info("Launching Snowflake Loader. Fetching state from DynamoDB")
          state <- ProcessManifest.initState[IO](config.awsRegion)
          _ <- Logger[IO].info("State fetched, acquiring DB connection")
          manifest = ProcessManifest.awsSyncProcessManifest[IO](state)
          connection <- database.getConnection(config, appName)
          _ <- Logger[IO].info("DB connection acquired. Loading...")
          exit <- Loader.run[IO](connection, config)(Sync[IO], database, manifest, logger)
        } yield exit
      case Right(Cli.Loader.Setup(config, skip, dryRun, debug, appName)) =>
        implicit val L: Logger[IO] = Logger.initLogger[IO](debug)
        implicit val D: Database[IO] = Database.init(dryRun)
        Logger[IO].info("Setting up...") *> Initializer.run[IO](config, skip, appName)
      case Right(Cli.Loader.Migrate(config, version, dryRun, debug, appName)) =>
        implicit val L: Logger[IO] = Logger.initLogger[IO](debug)
        implicit val D: Database[IO] = Database.init(dryRun)
        Logger[IO].info("Migrating...") *> Migrator.run[IO](config, version, appName)
      case Left(error) =>
        IO.delay(System.err.println(error)).as(ExitCode.Error)
    }
}
