/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import io.circe.{Json, JsonNumber, JsonObject}

import java.util.{List => JList, Map => JMap}
import scala.jdk.CollectionConverters._

/** Converts circe Json values into objects which are compatible with the snowflake ingest sdk */
object SnowflakeJsonFolder extends Json.Folder[AnyRef] {

  override def onNull: Null                                 = null
  override def onBoolean(value: Boolean): java.lang.Boolean = Boolean.box(value)
  override def onNumber(value: JsonNumber): AnyRef =
    tryBigInt(value).orElse(tryBigDecimal(value)).getOrElse(Double.box(value.toDouble))
  override def onString(value: String): String = value
  override def onArray(value: Vector[Json]): JList[AnyRef] =
    value.map[AnyRef](_.foldWith(this)).asJava
  override def onObject(value: JsonObject): JMap[String, AnyRef] =
    value.toMap
      .flatMap[String, AnyRef] {
        case (_, v) if v.isNull =>
          // Drop null values, to achieve more efficient indexing in the target Snowflake table
          None
        case (k, v) =>
          Some(k -> v.foldWith(this))
      }
      .asJava

  private def tryBigInt(v: JsonNumber): Option[java.math.BigInteger] =
    v.toBigInt.map(_.bigInteger)

  private def tryBigDecimal(v: JsonNumber): Option[java.math.BigDecimal] =
    v.toBigDecimal.map(_.bigDecimal)
}
