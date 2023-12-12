/*
 * Copyright (c) 2014-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package com.snowplowanalytics.snowplow.snowflake

import cats.effect.{Async, Sync}
import cats.implicits._
import doobie.Transactor
import net.snowflake.ingest.utils.{Utils => SnowflakeSdkUtils}

import java.security.PrivateKey
import java.util.Properties

object JdbcTransactor {

  private val driver: String = "net.snowflake.client.jdbc.SnowflakeDriver"

  def make[F[_]: Async](config: Config.Snowflake): F[Transactor[F]] =
    for {
      privateKey <- parsePrivateKey[F](config)
      props = jdbcProperties(config, privateKey)
    } yield Transactor.fromDriverManager[F](driver, config.url.getJdbcUrl, props, None)

  private def parsePrivateKey[F[_]: Sync](config: Config.Snowflake): F[PrivateKey] =
    Sync[F].delay { // Wrap in Sync because these can raise exceptions
      config.privateKeyPassphrase match {
        case Some(passphrase) =>
          SnowflakeSdkUtils.parseEncryptedPrivateKey(config.privateKey, passphrase)
        case None =>
          SnowflakeSdkUtils.parsePrivateKey(config.privateKey)
      }
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
