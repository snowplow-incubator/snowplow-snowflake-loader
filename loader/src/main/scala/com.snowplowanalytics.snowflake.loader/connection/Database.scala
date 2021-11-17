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
package com.snowplowanalytics.snowflake.loader.connection

import java.sql.{Connection => JdbcConnection}

import cats.Functor
import cats.syntax.functor._
import cats.effect.IO

import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.core.Config

import com.snowplowanalytics.snowflake.loader.connection.Database.Connection

/** DB-connection adapter */
trait Database[F[_]] {
  def getConnection(config: Config, appName: String): F[Connection]
  def execute[S: Statement](connection: Connection, ast: S): F[Unit]
  def startTransaction(connection: Connection, name: Option[String]): F[Unit]
  def commitTransaction(connection: Connection): F[Unit]
  def rollbackTransaction(connection: Connection): F[Unit]
  def executeAndOutput[S: Statement](connection: Connection, ast: S): F[Unit]
  def executeAndCountRows[S: Statement](connection: Connection, ast: S): F[Int]
  def executeAndReturnResult[S: Statement](connection: Connection, ast: S): F[List[Map[String, Object]]]

  def describeTable(connection: Connection, schema: String, table: String): F[List[Either[String, Column]]]
}

object Database {

  def apply[F[_]](implicit ev: Database[F]): Database[F] = ev

  def init(dryRun: Boolean): Database[IO] =
    if (dryRun) DryRun.init else Jdbc.init[IO]

  sealed trait Connection extends Product with Serializable
  object Connection {
    case class Jdbc private(conn: JdbcConnection) extends Connection
    case class Dry(conn: DryRun.Connection) extends Connection
  }

  /** Check state of the table and get a list of warnings (unmatches) if any */
  def checkState[F[_]: Database: Functor](conn: Connection, schema: String, table: String): F[List[String]] =
    Database[F].describeTable(conn, schema, table).map(AtomicDef.compare)
}
