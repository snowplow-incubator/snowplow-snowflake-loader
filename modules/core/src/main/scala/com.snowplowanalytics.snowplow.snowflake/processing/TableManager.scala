/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
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
    xa: Transactor[F]
  ): TableManager[F] = new TableManager[F] {

    def addColumns(columns: List[String]): F[Unit] =
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
