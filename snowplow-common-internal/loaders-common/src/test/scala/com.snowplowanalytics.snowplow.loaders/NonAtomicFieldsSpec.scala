/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.effect.IO
import org.specs2.Specification
import cats.effect.testing.specs2.CatsEffect

import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.iglu.client.resolver.registries.JavaNetRegistryLookup._
import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.snowplowanalytics.iglu.schemaddl.parquet.Type.Nullability.{Nullable, Required}

class NonAtomicFieldsSpec extends Specification with CatsEffect {

  def is = s2"""
  NonAtomicFields should
    resolve types for known schemas in unstruct_event $e1
    resolve types for known schemas in contexts $e2
  """

  def e1 = {
    val resolver = Resolver[IO](Nil, None)

    val tabledEntity = TabledEntity(TabledEntity.UnstructEvent, "myvendor", "myschema", 7)

    val entities = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expectedStruct = Type.Struct(
      List(
        Field("col_a", Type.String, Required),
        Field("col_c", Type.String, Nullable),
        Field("col_b", Type.String, Nullable)
      )
    )

    val expectedField = Field("unstruct_event_myvendor_myschema_7", expectedStruct, Nullable)

    val expected = TypedTabledEntity(
      tabledEntity,
      expectedField,
      Set((0, 0), (0, 1), (1, 0)),
      Nil
    )

    NonAtomicFields.resolveTypes(resolver, entities).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }

  def e2 = {
    val resolver = Resolver[IO](Nil, None)

    val tabledEntity = TabledEntity(TabledEntity.Context, "myvendor", "myschema", 7)

    val entities = Map(
      tabledEntity -> Set((0, 0), (0, 1), (1, 0))
    )

    val expectedStruct = Type.Struct(
      List(
        Field("_schema_version", Type.String, Required),
        Field("col_a", Type.String, Required),
        Field("col_c", Type.String, Nullable),
        Field("col_b", Type.String, Nullable)
      )
    )

    val expectedArray = Type.Array(expectedStruct, Required)

    val expectedField = Field("contexts_myvendor_myschema_7", expectedArray, Nullable)

    val expected = TypedTabledEntity(
      tabledEntity,
      expectedField,
      Set((0, 0), (0, 1), (1, 0)),
      Nil
    )

    NonAtomicFields.resolveTypes(resolver, entities).map { case NonAtomicFields.Result(fields, failures) =>
      (failures must beEmpty) and
        (fields must haveSize(1)) and
        (fields.head must beEqualTo(expected))
    }

  }
}
