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
import doobie.Transactor
import net.snowflake.ingest.utils.{Utils => SnowflakeSdkUtils}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.security.PrivateKey
import java.util.Properties

import com.snowplowanalytics.snowplow.snowflake.{Alert, AppHealth, Config, Monitoring}

object JdbcTransactor {

  private val driver: String = "net.snowflake.client.jdbc.SnowflakeDriver"

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def make[F[_]: Async](
    config: Config.Snowflake,
    monitoring: Monitoring[F],
    appHealth: AppHealth[F]
  ): F[Transactor[F]] =
    for {
      privateKey <- parsePrivateKey[F](config, monitoring, appHealth)
      props = jdbcProperties(config, privateKey)
    } yield Transactor.fromDriverManager[F](driver, config.url.jdbc, props, None)

  private def parsePrivateKey[F[_]: Async](
    config: Config.Snowflake,
    monitoring: Monitoring[F],
    appHealth: AppHealth[F]
  ): F[PrivateKey] =
    Sync[F]
      .delay { // Wrap in Sync because these can raise exceptions
        config.privateKeyPassphrase match {
          case Some(passphrase) =>
            SnowflakeSdkUtils.parseEncryptedPrivateKey(config.privateKey, passphrase)
          case None =>
            SnowflakeSdkUtils.parsePrivateKey(config.privateKey)
        }
      }
      .onError { e =>
        Logger[F].error(e)("Could not parse the Snowflake private key. Will do nothing but wait for loader to be killed") *>
          appHealth.setServiceHealth(AppHealth.Service.Snowflake, false) *>
          // This is a type of "setup" error, so we send a monitoring alert
          monitoring.alert(Alert.FailedToParsePrivateKey(e)) *>
          // We don't want to crash and exit, because we don't want to spam Sentry with exceptions about setup errors.
          // But there's no point in continuing or retrying. Instead we just block the fiber so the health probe appears unhealthy.
          Async[F].never
      }

  private def jdbcProperties(config: Config.Snowflake, privateKey: PrivateKey): Properties = {
    val props = new Properties()
    props.setProperty("user", config.user)
    props.put("privateKey", privateKey)
    props.setProperty("timezone", "UTC")
    config.role.foreach(props.setProperty("role", _))
    props.put("loginTimeout", config.jdbcLoginTimeout.toSeconds.toInt)
    props.put("networkTimeout", config.jdbcNetworkTimeout.toMillis.toInt)
    props.put("queryTimeout", config.jdbcQueryTimeout.toSeconds.toInt)
    props
  }
}
