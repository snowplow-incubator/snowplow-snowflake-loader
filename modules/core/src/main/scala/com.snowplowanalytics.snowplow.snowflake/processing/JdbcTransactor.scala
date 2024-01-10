/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake

import cats.effect.{Async, Sync}
import cats.implicits._
import doobie.Transactor
import net.snowflake.ingest.utils.{Utils => SnowflakeSdkUtils}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.security.PrivateKey
import java.util.Properties

object JdbcTransactor {

  private val driver: String = "net.snowflake.client.jdbc.SnowflakeDriver"

  private implicit def logger[F[_]: Sync] = Slf4jLogger.getLogger[F]

  def make[F[_]: Async](config: Config.Snowflake, monitoring: Monitoring[F]): F[Transactor[F]] =
    for {
      privateKey <- parsePrivateKey[F](config, monitoring)
      props = jdbcProperties(config, privateKey)
    } yield Transactor.fromDriverManager[F](driver, config.url.getJdbcUrl, props, None)

  private def parsePrivateKey[F[_]: Async](config: Config.Snowflake, monitoring: Monitoring[F]): F[PrivateKey] =
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
