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

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.instances.all._
import cats.effect.{ Sync, ExitCode }

import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.loader.connection.Database
import com.snowplowanalytics.snowflake.loader.ast.AlterTable.AlterColumnDatatype

/** Module containing functions to migrate Snowflake tables */
object Migrator {

  /** Run migration process */
  def run[F[_]: Sync: Database: Logger](config: Config, loaderVersion: String, appName: String): F[ExitCode] = {
    def alterColumn(connection: Database.Connection, column: String, columnType: SnowflakeDatatype): F[Unit] =
      Database[F].executeAndOutput(connection, AlterColumnDatatype(config.schema, Defaults.Table, column, columnType))

    loaderVersion match {
      case "0.4.0" =>
        for {
          connection <- Database[F].getConnection(config, appName)
          _ <- alterColumn(connection, "user_ipaddress", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "user_fingerprint", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "domain_userid", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "network_userid", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "geo_region", SnowflakeDatatype.Varchar(Some(3)))
          _ <- alterColumn(connection, "ip_organization", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "ip_domain", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "refr_domain_userid", SnowflakeDatatype.Varchar(Some(128)))
          _ <- alterColumn(connection, "domain_sessionid", SnowflakeDatatype.Varchar(Some(128)))
        } yield ExitCode.Success
      case _ =>
        val message = s"Unrecognized Snowplow Snowflake Loader version: $loaderVersion. (Supported: 0.4.0)"
        Logger[F].error(message).as(ExitCode.Error)
    }
  }
}
