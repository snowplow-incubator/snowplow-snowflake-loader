/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.effect.Sync
import cats.implicits._

import com.snowplowanalytics.iglu.client.resolver.registries.RegistryLookup
import com.snowplowanalytics.iglu.client.resolver.Resolver
import com.snowplowanalytics.snowplow.badrows.FailureDetails

object NonAtomicFields {

  /**
   * Describes the Field Types (not data) present in a batch of events
   *
   * @param fields
   *   field type information about each family of Iglu schema. E.g. if a batch contains versions
   *   1-0-0, 1-0-1 and 1-1-0 of a schema, they will be present as a single item of this list. If
   *   the batch also contains version 2-0-0 of that schema, it will be present as an extra item of
   *   this list.
   * @param igluFailures
   *   details of schemas that were present in the batch but could not be looked up by the Iglu
   *   resolver.
   */
  case class Result(fields: List[TypedTabledEntity], igluFailures: List[ColumnFailure])

  /**
   * Describes a failure to lookup a series of Iglu schemas
   *
   * @param tabledEntity
   *   The family of iglu schemas for which the lookup was needed
   * @param versionsInBatch
   *   The schema versions for which a lookup was needed
   * @param failure
   *   Why the lookup failed
   */
  case class ColumnFailure(
    tabledEntity: TabledEntity,
    versionsInBatch: Set[SchemaSubVersion],
    failure: FailureDetails.LoaderIgluError
  )

  // TODO: does this belong here?
  def resolveTypes[F[_]: Sync: RegistryLookup](
    resolver: Resolver[F],
    entities: Map[TabledEntity, Set[SchemaSubVersion]]
  ): F[Result] =
    entities.toList
      .traverse { case (tabledEntity, subVersions) =>
        SchemaProvider
          .fetchSchemasWithSameModel(resolver, TabledEntity.toSchemaKey(tabledEntity, subVersions.max))
          .map(TypedTabledEntity.build(tabledEntity, subVersions, _))
          .leftMap(ColumnFailure(tabledEntity, subVersions, _))
          .value
      }
      .map { eithers =>
        val (failures, good) = eithers.separate
        Result(good, failures)
      }
}
