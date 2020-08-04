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
package connection

import java.sql.{ DriverManager, Connection }

import java.util.Properties

import scala.collection.mutable.ListBuffer

import cats.effect.Sync

import com.snowplowanalytics.snowflake.loader.ast._
import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

object Jdbc {

  def init[F[_]: Sync]: Database[F] = new Database[F] {
    def getConnection(config: Config): F[Database.Connection] = Sync[F].delay {
      Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

      /**
       * A list of AWS region names that Snowflake connection string don't have `aws` subdomain for
       * See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
       */
      val regionsWithoutAwsSubdomain = List("us-east-1", "eu-west-1", "eu-central-1", "ap-southeast-1", "ap-southeast-2")

      /**
       * Host corresponds to Snowflake full account name which might include cloud platform and region
       * See https://docs.snowflake.com/en/user-guide/jdbc-configure.html#connection-parameters
       */
      val host = config.jdbcHost match {
        case Some(overrideHost) => overrideHost
        case None =>
          if (config.snowflakeRegion == "us-west-2")
            s"${config.account}.snowflakecomputing.com"
          else if (regionsWithoutAwsSubdomain.contains(config.snowflakeRegion))
            s"${config.account}.${config.snowflakeRegion}.snowflakecomputing.com"
          else
            s"${config.account}.${config.snowflakeRegion}.aws.snowflakecomputing.com"
      }

      // Build connection properties
      val properties = new Properties()

      val password = config.password match {
        case Config.PasswordConfig.PlainText(text) => text
        case Config.PasswordConfig.EncryptedKey(Config.EncryptedConfig(key)) =>
          PasswordService.getKey(key.parameterName) match {
            case Right(result) => result
            case Left(error) =>
              throw new RuntimeException(s"Cannot retrieve JDBC password from EC2 Parameter Store. $error")
          }
      }

      val userAgent = ProjectMetadata.name + "/" + ProjectMetadata.version

      properties.put("user", config.username)
      properties.put("password", password)
      properties.put("account", config.account)
      properties.put("warehouse", config.warehouse)
      properties.put("db", config.database)
      properties.put("schema", config.schema)
      properties.put("userAgent", userAgent)

      val connectStr = s"jdbc:snowflake://$host"
      Database.Connection.Jdbc(DriverManager.getConnection(connectStr, properties))
    }

    /** Execute SQL statement */
    def execute[S: Statement](connection: Database.Connection, ast: S): F[Unit] =
      run(connection) { conn =>
        Sync[F].delay {
          val jdbcStatement = conn.createStatement()
          jdbcStatement.execute(ast.getStatement.value)
          jdbcStatement.close()
        }
      }

    /** Begin transaction */
    def startTransaction(connection: Database.Connection, name: Option[String]): F[Unit] =
      run(connection) { conn =>
        Sync[F].delay {
          val jdbcStatement = conn.createStatement()
          jdbcStatement.execute(s"BEGIN TRANSACTION NAME ${name.getOrElse("")}")
          jdbcStatement.close()
        }
      }

    /** Commit transaction */
    def commitTransaction(connection: Database.Connection): F[Unit] =
      run(connection) { conn =>
        Sync[F].delay {
          val jdbcStatement = conn.createStatement()
          jdbcStatement.execute("COMMIT")
          jdbcStatement.close()
        }
      }

    def rollbackTransaction(connection: Database.Connection): F[Unit] =
      run(connection) { conn =>
        Sync[F].delay {
          val jdbcStatement = conn.createStatement()
          jdbcStatement.execute("ROLLBACK")
          jdbcStatement.close()
        }
      }

    /** Execute SQL statement and print status */
    def executeAndOutput[S: Statement](connection: Database.Connection, ast: S): F[Unit] =
      run(connection) { conn =>
        Sync[F].delay {
          val statement = conn.createStatement()
          val rs = statement.executeQuery(ast.getStatement.value)
          while (rs.next()) {
            println(rs.getString("status"))
          }
          statement.close()
        }
      }

    /** Execute SQL query and count rows */
    def executeAndCountRows[S: Statement](connection: Database.Connection, ast: S): F[Int] =
      run(connection) { conn =>
        Sync[F].delay {
          val statement = conn.createStatement()
          val rs = statement.executeQuery(ast.getStatement.value)
          var i = 0
          while (rs.next()) {
            i = i + 1
          }
          i
        }
      }

    /** Execute SQL query and return result */
    def executeAndReturnResult[S: Statement](connection: Database.Connection, ast: S): F[List[Map[String, Object]]] =
      run(connection) { conn =>
        Sync[F].delay {
          val statement = conn.createStatement()
          val rs = statement.executeQuery(ast.getStatement.value)
          val metadata = rs.getMetaData
          val result = new ListBuffer[Map[String, Object]]()

          while (rs.next () ) {
            val row = (for (i <- 1 to metadata.getColumnCount) yield metadata.getColumnName(i) -> rs.getObject (i) ).toMap
            result += row
          }

          rs.close ()
          statement.close ()
          result.toList
        }
      }

    private def run[A](connection: Database.Connection)(f: Connection => F[A]): F[A] =
      connection match {
        case Database.Connection.Jdbc(conn) => f(conn)
        case Database.Connection.Dry(_) =>
          Sync[F].raiseError(new IllegalStateException("JDBC Database was called with DryRun connection"))
      }
  }
}
