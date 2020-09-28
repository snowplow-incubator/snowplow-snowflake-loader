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

import cats.effect.{ExitCode, IO, IOApp, Sync}

import com.snowplowanalytics.snowflake.core.{Cli, ProcessManifest}
import com.snowplowanalytics.snowflake.loader.connection.Database

object Main extends IOApp {
  def run(args: List[String]): IO[ExitCode] =
    Cli.Loader.parse(args).value.flatMap {
      case Right(Cli.Loader.Load(config, dryRun)) =>
        val database = Database.init(dryRun)
        for {
          state <- ProcessManifest.initState[IO](config.awsRegion)
          manifest = ProcessManifest.awsSyncProcessManifest[IO](state)
          connection <- database.getConnection(config)
          _ <- IO.delay(println("Loading..."))
          exit <- Loader.run[IO](connection, config)(Sync[IO], database, manifest)
        } yield exit
      case Right(Cli.Loader.Setup(config, skip, dryRun)) =>
        implicit val D: Database[IO] = Database.init(dryRun)
        IO.delay(println("Setting up...")) *> Initializer.run[IO](config, skip)
      case Right(Cli.Loader.Migrate(config, version, dryRun)) =>
        implicit val D: Database[IO] = Database.init(dryRun)
        IO.delay(println("Migrating...")) *> Migrator.run[IO](config, version)
      case Left(error) =>
        IO.delay(System.err.println(error)).as(ExitCode.Error)
    }
}
