/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake.processing

import cats.effect.{Async, Sync}
import cats.implicits._
import com.snowplowanalytics.snowplow.snowflake.{Alert, AppHealth, Config, Monitoring}
import doobie.implicits._
import doobie.{ConnectionIO, Fragment}
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import com.snowplowanalytics.snowplow.loaders.transform.AtomicFields

import scala.util.matching.Regex

trait TableManager[F[_]] {

  def initializeEventsTable(): F[Unit]

  def addColumns(columns: List[String]): F[Unit]

}

object TableManager {

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def make[F[_]: Async](
    config: Config.Snowflake,
    appHealth: AppHealth[F],
    retriesConfig: Config.Retries,
    monitoring: Monitoring[F]
  ): F[TableManager[F]] =
    JdbcTransactor.make(config, monitoring, appHealth).map { transactor =>
      new TableManager[F] {

        override def initializeEventsTable(): F[Unit] =
          SnowflakeRetrying.withRetries(appHealth, retriesConfig, monitoring, Alert.FailedToCreateEventsTable(_)) {
            Logger[F].info(s"Opening JDBC connection to ${config.url.jdbc}") *>
              executeInitTableQuery()
          }

        override def addColumns(columns: List[String]): F[Unit] =
          SnowflakeRetrying.withRetries(appHealth, retriesConfig, monitoring, Alert.FailedToAddColumns(columns, _)) {
            for {
              _ <- Logger[F].info(s"Altering table to add columns [${columns.mkString(", ")}]")
              addable <- columns.traverse(addableColumn(appHealth, monitoring, _))
              _ <- executeAddColumnsQuery(addable)
            } yield ()
          }

        def executeInitTableQuery(): F[Unit] = {
          val tableName = fqTableName(config)

          transactor.rawTrans
            .apply {
              Logger[ConnectionIO].info(s"Creating table $tableName if it does not already exist...") *>
                sqlCreateTable(tableName).update.run.void
            }
            .recoverWith {
              case sql: java.sql.SQLException if sql.getErrorCode === 3001 =>
                Logger[F].info(s"Access denied when trying to create table. Will ignore error and assume table already exists.")
            }
        }

        def executeAddColumnsQuery(columns: List[AddableColumn]): F[Unit] =
          transactor.rawTrans.apply {
            columns.traverse_ { column =>
              sqlAlterTable(config, column).update.run.void
                .recoverWith {
                  case e: SnowflakeSQLException if e.getErrorCode === 1430 =>
                    Logger[ConnectionIO].info(show"Column already exists: ${column.name}")
                }
            }
          }
      }
    }

  private sealed trait AddableColumn {
    def name: String
  }
  private object AddableColumn {
    case class UnstructEvent(name: String) extends AddableColumn
    case class Contexts(name: String) extends AddableColumn
  }

  private def addableColumn[F[_]: Async](
    appHealth: AppHealth[F],
    monitoring: Monitoring[F],
    name: String
  ): F[AddableColumn] =
    name match {
      case reUnstruct() => Sync[F].pure(AddableColumn.UnstructEvent(name))
      case reContext()  => Sync[F].pure(AddableColumn.Contexts(name))
      case other if AtomicFields.withLoadTstamp.exists(_.name === other) =>
        Logger[F].error(s"Table is missing required field $name. Will do nothing but wait for loader to be killed") *>
          appHealth.setServiceHealth(AppHealth.Service.Snowflake, false) *>
          // This is a type of "setup" error, so we send a monitoring alert
          monitoring.alert(Alert.TableIsMissingAtomicColumn(name)) *>
          // We don't want to crash and exit, because we don't want to spam Sentry with exceptions about setup errors.
          // But there's no point in continuing or retrying. Instead we just block the fiber so the health probe appears unhealthy.
          Async[F].never
      case other =>
        Sync[F].raiseError(new IllegalStateException(s"Cannot alter table to add unrecognized column $other"))
    }

  private val reUnstruct: Regex = "^unstruct_event_.*$".r
  private val reContext: Regex  = "^contexts_.*$".r

  private def sqlAlterTable(config: Config.Snowflake, addableColumn: AddableColumn): Fragment = {
    val tableName = fqTableName(config)
    val colType = addableColumn match {
      case AddableColumn.UnstructEvent(_) => "OBJECT"
      case AddableColumn.Contexts(_)      => "ARRAY"
    }
    val colTypeFrag = Fragment.const0(colType)
    val colNameFrag = Fragment.const0(addableColumn.name)
    sql"""
    ALTER TABLE identifier($tableName)
    ADD COLUMN $colNameFrag $colTypeFrag
    """
  }

  // fully qualified name
  private def fqTableName(config: Config.Snowflake): String =
    s"${config.database}.${config.schema}.${config.table}"

  private def sqlCreateTable(tableName: String): Fragment =
    sql"""
    CREATE TABLE IF NOT EXISTS identifier($tableName) (
      app_id                      VARCHAR,
      platform                    VARCHAR,
      etl_tstamp                  TIMESTAMP_NTZ,
      collector_tstamp            TIMESTAMP_NTZ  NOT NULL,
      dvce_created_tstamp         TIMESTAMP_NTZ,
      event                       VARCHAR,
      event_id                    VARCHAR        NOT NULL UNIQUE,
      txn_id                      INTEGER,
      name_tracker                VARCHAR,
      v_tracker                   VARCHAR,
      v_collector                 VARCHAR        NOT NULL,
      v_etl                       VARCHAR        NOT NULL,
      user_id                     VARCHAR,
      user_ipaddress              VARCHAR,
      user_fingerprint            VARCHAR,
      domain_userid               VARCHAR,
      domain_sessionidx           SMALLINT,
      network_userid              VARCHAR,
      geo_country                 VARCHAR,
      geo_region                  VARCHAR,
      geo_city                    VARCHAR,
      geo_zipcode                 VARCHAR,
      geo_latitude                DOUBLE PRECISION,
      geo_longitude               DOUBLE PRECISION,
      geo_region_name             VARCHAR,
      ip_isp                      VARCHAR,
      ip_organization             VARCHAR,
      ip_domain                   VARCHAR,
      ip_netspeed                 VARCHAR,
      page_url                    VARCHAR,
      page_title                  VARCHAR,
      page_referrer               VARCHAR,
      page_urlscheme              VARCHAR,
      page_urlhost                VARCHAR,
      page_urlport                INTEGER,
      page_urlpath                VARCHAR,
      page_urlquery               VARCHAR,
      page_urlfragment            VARCHAR,
      refr_urlscheme              VARCHAR,
      refr_urlhost                VARCHAR,
      refr_urlport                INTEGER,
      refr_urlpath                VARCHAR,
      refr_urlquery               VARCHAR,
      refr_urlfragment            VARCHAR,
      refr_medium                 VARCHAR,
      refr_source                 VARCHAR,
      refr_term                   VARCHAR,
      mkt_medium                  VARCHAR,
      mkt_source                  VARCHAR,
      mkt_term                    VARCHAR,
      mkt_content                 VARCHAR,
      mkt_campaign                VARCHAR,
      se_category                 VARCHAR,
      se_action                   VARCHAR,
      se_label                    VARCHAR,
      se_property                 VARCHAR,
      se_value                    DOUBLE PRECISION,
      tr_orderid                  VARCHAR,
      tr_affiliation              VARCHAR,
      tr_total                    NUMBER(18,2),
      tr_tax                      NUMBER(18,2),
      tr_shipping                 NUMBER(18,2),
      tr_city                     VARCHAR,
      tr_state                    VARCHAR,
      tr_country                  VARCHAR,
      ti_orderid                  VARCHAR,
      ti_sku                      VARCHAR,
      ti_name                     VARCHAR,
      ti_category                 VARCHAR,
      ti_price                    NUMBER(18,2),
      ti_quantity                 INTEGER,
      pp_xoffset_min              INTEGER,
      pp_xoffset_max              INTEGER,
      pp_yoffset_min              INTEGER,
      pp_yoffset_max              INTEGER,
      useragent                   VARCHAR,
      br_name                     VARCHAR,
      br_family                   VARCHAR,
      br_version                  VARCHAR,
      br_type                     VARCHAR,
      br_renderengine             VARCHAR,
      br_lang                     VARCHAR,
      br_features_pdf             BOOLEAN,
      br_features_flash           BOOLEAN,
      br_features_java            BOOLEAN,
      br_features_director        BOOLEAN,
      br_features_quicktime       BOOLEAN,
      br_features_realplayer      BOOLEAN,
      br_features_windowsmedia    BOOLEAN,
      br_features_gears           BOOLEAN,
      br_features_silverlight     BOOLEAN,
      br_cookies                  BOOLEAN,
      br_colordepth               VARCHAR,
      br_viewwidth                INTEGER,
      br_viewheight               INTEGER,
      os_name                     VARCHAR,
      os_family                   VARCHAR,
      os_manufacturer             VARCHAR,
      os_timezone                 VARCHAR,
      dvce_type                   VARCHAR,
      dvce_ismobile               BOOLEAN,
      dvce_screenwidth            INTEGER,
      dvce_screenheight           INTEGER,
      doc_charset                 VARCHAR,
      doc_width                   INTEGER,
      doc_height                  INTEGER,
      tr_currency                 VARCHAR,
      tr_total_base               NUMBER(18, 2),
      tr_tax_base                 NUMBER(18, 2),
      tr_shipping_base            NUMBER(18, 2),
      ti_currency                 VARCHAR,
      ti_price_base               NUMBER(18, 2),
      base_currency               VARCHAR,
      geo_timezone                VARCHAR,
      mkt_clickid                 VARCHAR,
      mkt_network                 VARCHAR,
      etl_tags                    VARCHAR,
      dvce_sent_tstamp            TIMESTAMP_NTZ,
      refr_domain_userid          VARCHAR,
      refr_dvce_tstamp            TIMESTAMP_NTZ,
      domain_sessionid            VARCHAR,
      derived_tstamp              TIMESTAMP_NTZ,
      event_vendor                VARCHAR,
      event_name                  VARCHAR,
      event_format                VARCHAR,
      event_version               VARCHAR,
      event_fingerprint           VARCHAR,
      true_tstamp                 TIMESTAMP_NTZ,
      load_tstamp                 TIMESTAMP_NTZ,
      CONSTRAINT event_id_pk PRIMARY KEY(event_id)
    )
  """
}
