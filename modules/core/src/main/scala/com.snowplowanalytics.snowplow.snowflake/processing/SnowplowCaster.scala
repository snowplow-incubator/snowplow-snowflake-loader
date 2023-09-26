/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.snowflake.processing

import io.circe.Json

import java.time.{Instant, LocalDate, OffsetDateTime, ZoneOffset}
import java.util.{List => JList, Map => JMap}

import com.snowplowanalytics.iglu.schemaddl.parquet.Type
import com.snowplowanalytics.iglu.schemaddl.parquet.Caster

import scala.jdk.CollectionConverters._

/** Converts schema-ddl values into objects which are compatible with the snowflake ingest sdk */
private[processing] object SnowflakeCaster extends Caster[AnyRef] {

  override def nullValue: Null                             = null
  override def jsonValue(v: Json): String                  = v.noSpaces
  override def stringValue(v: String): String              = v
  override def booleanValue(v: Boolean): java.lang.Boolean = Boolean.box(v)
  override def intValue(v: Int): java.lang.Integer         = Int.box(v)
  override def longValue(v: Long): java.lang.Long          = Long.box(v)
  override def doubleValue(v: Double): java.lang.Double    = Double.box(v)
  override def decimalValue(unscaled: BigInt, details: Type.Decimal): java.math.BigDecimal =
    new java.math.BigDecimal(unscaled.bigInteger, details.scale)
  override def timestampValue(v: Instant): OffsetDateTime  = OffsetDateTime.ofInstant(v, ZoneOffset.UTC)
  override def dateValue(v: LocalDate): LocalDate          = v
  override def arrayValue(vs: List[AnyRef]): JList[AnyRef] = vs.asJava
  override def structValue(vs: List[Caster.NamedValue[AnyRef]]): JMap[String, AnyRef] =
    vs.map { case Caster.NamedValue(k, v) =>
      (k, v)
    }.toMap
      .asJava

}
