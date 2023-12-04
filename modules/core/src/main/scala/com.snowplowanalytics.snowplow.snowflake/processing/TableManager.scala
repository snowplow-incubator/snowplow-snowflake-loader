/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import cats.implicits._
import cats.effect.{Async, Sync}
import doobie.{ConnectionIO, Fragment, Transactor}
import doobie.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import net.snowflake.client.jdbc.SnowflakeSQLException

import com.snowplowanalytics.snowplow.snowflake.{Config, SQLUtils}

import scala.util.matching.Regex

trait TableManager[F[_]] {

  def addColumns(columns: List[String]): F[Unit]

}

object TableManager {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def fromTransactor[F[_]: Async](
    config: Config.Snowflake,
    xa: Transactor[F],
    snowflakeHealth: SnowflakeHealth[F],
    retriesConfig: Config.Retries
  ): TableManager[F] = new TableManager[F] {

    def addColumns(columns: List[String]): F[Unit] = SnowflakeRetrying.retryIndefinitely(snowflakeHealth, retriesConfig) {
      Logger[F].info(s"Altering table to add columns [${columns.mkString(", ")}]") *>
        xa.rawTrans.apply {
          columns.traverse_ { col =>
            sqlAlterTable(config, col).update.run.void
              .recoverWith {
                case e: SnowflakeSQLException if e.getErrorCode === 1430 =>
                  Logger[ConnectionIO].info(show"Column already exists: $col")
              }
          }
        }
    }
  }

  private val reUnstruct: Regex = "^unstruct_event_.*$".r
  private val reContext: Regex  = "^contexts_.*$".r

  private def sqlAlterTable(config: Config.Snowflake, colName: String): Fragment = {
    val tableName = SQLUtils.fqTableName(config)
    val colType = colName match {
      case reUnstruct() => "OBJECT"
      case reContext()  => "ARRAY"
      case other        => throw new IllegalStateException(s"Cannot alter table to add column $other")
    }
    val colTypeFrag = Fragment.const0(colType)
    val colNameFrag = Fragment.const0(colName)
    sql"""
    ALTER TABLE identifier($tableName)
    ADD COLUMN $colNameFrag $colTypeFrag
    """
  }

}
