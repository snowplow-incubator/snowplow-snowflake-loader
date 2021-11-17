/*
 * Copyright (c) 2017-2021 Snowplow Analytics Ltd. All rights reserved.
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

import cats.effect.{ Sync, ExitCode }
import cats.syntax.flatMap._
import cats.syntax.functor._

import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.core.Config.SetupSteps
import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.loader.connection.Database

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run[F[_]: Sync: Database](config: Config, skip: Set[SetupSteps], appName: String): F[ExitCode] = {
    def execute[S: Statement](connection: Database.Connection, step: SetupSteps, statement: S): F[Unit] =
      if (!skip.contains(step)) Database[F].executeAndOutput(connection, statement) else Sync[F].unit

    for {
      connection <- Database[F].getConnection(config, appName)
      credentials = PasswordService.getSetupCredentials(config.auth)
      _ <- execute(connection, SetupSteps.Schema, CreateSchema(config.schema))
      _ <- execute(connection, SetupSteps.Table, AtomicDef.getTable(config.schema))
      _ <- execute(connection, SetupSteps.Warehouse, CreateWarehouse(config.warehouse, Some(CreateWarehouse.XSmall), Some(300), Some(true)))
      _ <- execute(connection, SetupSteps.FileFormat, CreateFileFormat.CreateJsonFormat(Defaults.FileFormat))
      _ <- execute(connection, SetupSteps.Stage, CreateStage(config.stage, config.stageUrl, Defaults.FileFormat, config.schema, credentials))
    } yield ExitCode.Success
  }
}
