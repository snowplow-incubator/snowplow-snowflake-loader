/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.implicits._
import cats.data.NonEmptyList

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Migrations, Type}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.core.SchemaKey
import com.snowplowanalytics.snowplow.analytics.scalasdk.{Data => SdkData, SnowplowEvent}

import scala.math.abs

/**
 * Field type information for a family of versioned Iglu schemas which are treated as a common
 * entity when writing to the table
 *
 * E.g. unstruct events with types 1-0-0, 1-0-1, and 1-1-0 are merged into the same
 * TypedTabledEntity
 *
 * @param tabledEntity
 *   Identifier to this entity. Includes meta data but no type information.
 * @param mergedField
 *   The schema-ddl Field describing a merge of all schema versions in this group
 * @param mergedVersions
 *   The sub-versions (e.g. '*-0-0' and '*-0-1') which were successfully merged into the mergedField
 * @param recoveries
 *   The schema-ddl Fields for schema versions which could not be merged into the main mergedField
 */
case class TypedTabledEntity(
  tabledEntity: TabledEntity,
  mergedField: Field,
  mergedVersions: Set[SchemaSubVersion],
  recoveries: Map[SchemaSubVersion, Field]
)

object TypedTabledEntity {

  /**
   * Calculate the TypedTableEntity for a group of entities
   *
   * This is a pure function: we have already looked up schemas from Iglu.
   *
   * @param tabledEntity
   *   Identifier to this entity
   * @param subVersions
   *   Sub-versions (e.g. '*-0-0') that were present in the batch of events.
   * @param schemas
   *   Iglu schemas pre-fetched from Iglu Server
   */
  def build(
    tabledEntity: TabledEntity,
    subVersions: Set[SchemaSubVersion],
    schemas: NonEmptyList[SchemaWithKey]
  ): TypedTabledEntity = {
    // Schemas need to be ordered by key to merge in correct order.
    val NonEmptyList(root, tail) = schemas.sorted
    val columnGroup =
      TypedTabledEntity(tabledEntity, fieldFromSchema(tabledEntity, root.schema), Set(keyToSubVersion(root.schemaKey)), Map.empty)
    tail
      .map(schemaWithKey => (fieldFromSchema(tabledEntity, schemaWithKey.schema), schemaWithKey.schemaKey))
      .foldLeft(columnGroup) { case (columnGroup, (field, schemaKey)) =>
        val subversion = keyToSubVersion(schemaKey)
        Migrations.mergeSchemas(columnGroup.mergedField, field) match {
          case Left(_) =>
            if (subVersions.contains(subversion)) {
              val hash = abs(field.hashCode())
              // typedField always has a single element in matchingKeys
              val recoverPoint = schemaKey.version.asString.replaceAll("-", "_")
              val newName = s"${field.name}_recovered_${recoverPoint}_$hash"
              columnGroup.copy(recoveries = columnGroup.recoveries + (subversion -> field.copy(name = newName)))
            } else {
              // do not create a recovered column if that type were not in the batch
              columnGroup
            }
          case Right(mergedField) =>
            columnGroup.copy(mergedField = mergedField, mergedVersions = columnGroup.mergedVersions + subversion)
        }
      }
  }

  private def fieldFromSchema(tabledEntity: TabledEntity, schema: Schema): Field = {
    val sdkEntityType = tabledEntity.entityType match {
      case TabledEntity.UnstructEvent => SdkData.UnstructEvent
      case TabledEntity.Context => SdkData.Contexts(SdkData.CustomContexts)
    }
    val fieldName = SnowplowEvent.transformSchema(sdkEntityType, tabledEntity.vendor, tabledEntity.schemaName, tabledEntity.model)

    tabledEntity.entityType match {
      case TabledEntity.UnstructEvent =>
        Field.normalize {
          Field.build(fieldName, schema, enforceValuePresence = false)
        }
      case TabledEntity.Context =>
        // Must normalize first and add the schema key field after. To avoid unlikely weird issues
        // with similar existing keys.
        addSchemaVersionKey {
          Field.normalize {
            Field.buildRepeated(fieldName, schema, enforceItemPresence = true, Type.Nullability.Nullable)
          }
        }
    }
  }

  private def keyToSubVersion(key: SchemaKey): SchemaSubVersion = (key.version.revision, key.version.addition)

  private def addSchemaVersionKey(field: Field): Field = {
    val fieldType = field.fieldType match {
      case arr @ Type.Array(struct @ Type.Struct(subFields), _) =>
        val fixedFields = subFields
          .filter(_.name =!= "_schema_version") // Our special key takes priority over a key of the same name in the schema
          .prepended(Field("_schema_version", Type.String, Type.Nullability.Required))
        arr.copy(element = struct.copy(fields = fixedFields))
      case other =>
        // This is OK. It must be a weird schema, whose root type is not an object.
        // Unlikely but allowed according to our rules.
        other
    }
    field.copy(fieldType = fieldType)
  }
}
