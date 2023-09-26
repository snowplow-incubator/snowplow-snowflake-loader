/*
 * Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Snowplow Community License Version 1.0,
 * and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
 * You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
 */
package com.snowplowanalytics.snowplow.loaders

import cats.implicits._
import cats.data.{NonEmptyList, Validated, ValidatedNel}
import io.circe.Json

import com.snowplowanalytics.iglu.schemaddl.parquet.{Caster, Field, Type}
import com.snowplowanalytics.snowplow.badrows.{BadRow, Failure => BadRowFailure, FailureDetails, Processor => BadRowProcessor}
import com.snowplowanalytics.snowplow.badrows.{Payload => BadPayload}
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.iglu.schemaddl.parquet.CastError
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

object Transform {

  /**
   * Transform a Snowplow Event into types compatible with the loader target
   *
   * @param processor
   *   Details about this loader, only used for generating a bad row
   * @param caster
   *   Casts schema + JSON into an application-specific type
   * @param event
   *   The Snowplow Event received from the stream
   * @param batchInfo
   *   Pre-calculated Iglu types for a batch of events, used to guide the transformation
   * @return
   *   Application-specific types if iglu schemas were successfully resolved for this event, and the
   *   event's entities can be cast to the schema-ddl types. Otherwise a bad row.
   */
  def transformEvent[A](
    processor: BadRowProcessor,
    caster: Caster[A],
    event: Event,
    batchInfo: NonAtomicFields.Result
  ): Either[BadRow, List[Caster.NamedValue[A]]] =
    failForResolverErrors(processor, event, batchInfo.igluFailures) *>
      (forAtomic(caster, event), forEntities(caster, event, batchInfo.fields))
        .mapN { case (atomic, nonAtomic) =>
          atomic ::: nonAtomic
        }
        .toEither
        .leftMap { nel =>
          BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event))
        }

  /**
   * Transform a Snowplow Event into types compatible with the loader target
   *
   * Unlike `transformEvent`, this function is not guided by the Iglu Schema. It is appropriate for
   * destinations with non-strict column types, e.g. Snowflake.
   *
   * @param processor
   *   Details about this loader, only used for generating a bad row
   * @param caster
   *   Casts schema + JSON into an application-specific type
   * @param jsonCaster
   *   Casts JSON into an application-specific type, without regard for the Iglu schema
   * @param event
   *   The Snowplow Event received from the stream
   * @return
   *   Application-specific types if iglu schemas were successfully resolved for this event, and the
   *   event's entities can be cast to the schema-ddl types. Otherwise a bad row.
   */
  def transformEventUnstructured[A](
    processor: BadRowProcessor,
    caster: Caster[A],
    jsonCaster: Json.Folder[A],
    event: Event
  ): Either[BadRow, List[Caster.NamedValue[A]]] =
    forAtomic(caster, event).toEither
      .map { atomic =>
        val entities = event.contexts.toShreddedJson ++ event.derived_contexts.toShreddedJson ++ event.unstruct_event.toShreddedJson.toMap
        atomic ::: entities.toList.map { case (key, json) =>
          Caster.NamedValue(key, json.foldWith(jsonCaster))
        }
      }
      .leftMap { nel =>
        BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event))
      }

  private def failForResolverErrors(
    processor: BadRowProcessor,
    event: Event,
    failures: List[NonAtomicFields.ColumnFailure]
  ): Either[BadRow, Unit] = {
    val schemaFailures = failures.flatMap { case NonAtomicFields.ColumnFailure(tabledEntity, versionsInBatch, failure) =>
      tabledEntity.entityType match {
        case TabledEntity.UnstructEvent =>
          event.unstruct_event.data match {
            case Some(SelfDescribingData(schemaKey, _)) if keyMatches(tabledEntity, versionsInBatch, schemaKey) =>
              Some(failure)
            case _ =>
              None
          }
        case TabledEntity.Context =>
          val allContexts = event.contexts.data ::: event.derived_contexts.data
          if (allContexts.exists(context => keyMatches(tabledEntity, versionsInBatch, context.schema)))
            Some(failure)
          else
            None
      }
    }

    NonEmptyList.fromList(schemaFailures) match {
      case None => Right(())
      case Some(nel) =>
        Left(BadRow.LoaderIgluError(processor, BadRowFailure.LoaderIgluErrors(nel), BadPayload.LoaderPayload(event)))
    }
  }

  private def forEntities[A](
    caster: Caster[A],
    event: Event,
    entities: List[TypedTabledEntity]
  ): ValidatedNel[FailureDetails.LoaderIgluError, List[Caster.NamedValue[A]]] =
    entities.flatMap { case TypedTabledEntity(entity, field, subVersions, recoveries) =>
      val head = forEntity(caster, entity, field, subVersions, event)
      val tail = recoveries.toList.map { case (recoveryVersion, recoveryField) =>
        forEntity(caster, entity, recoveryField, Set(recoveryVersion), event)
      }
      head :: tail
    }.sequence

  private def forEntity[A](
    caster: Caster[A],
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[FailureDetails.LoaderIgluError, Caster.NamedValue[A]] = {
    val result = te.entityType match {
      case TabledEntity.UnstructEvent => forUnstruct(caster, te, field, subVersions, event)
      case TabledEntity.Context       => forContexts(caster, te, field, subVersions, event)
    }
    result
      .map { fieldValue =>
        Caster.NamedValue(field.name, fieldValue)
      }
      .leftMap { failure =>
        val schemaKey = TabledEntity.toSchemaKey(te, subVersions.max)
        castErrorToLoaderIgluError(schemaKey, failure)
      }
  }

  private def forUnstruct[A](
    caster: Caster[A],
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[CastError, A] =
    event.unstruct_event.data match {
      case Some(SelfDescribingData(schemaKey, unstructData)) if keyMatches(te, subVersions, schemaKey) =>
        Caster.cast(caster, field, unstructData)
      case _ =>
        Validated.Valid(caster.nullValue)
    }

  private def forContexts[A](
    caster: Caster[A],
    te: TabledEntity,
    field: Field,
    subVersions: Set[SchemaSubVersion],
    event: Event
  ): ValidatedNel[CastError, A] = {
    val allContexts = event.contexts.data ::: event.derived_contexts.data
    val matchingContexts = allContexts
      .filter(context => keyMatches(te, subVersions, context.schema))

    if (matchingContexts.nonEmpty) {
      val jsonArrayWithContexts = Json.fromValues(matchingContexts.map(jsonForContext).toVector)
      Caster.cast(caster, field, jsonArrayWithContexts)
    } else {
      Validated.Valid(caster.nullValue)
    }
  }

  private def keyMatches(
    te: TabledEntity,
    subVersions: Set[SchemaSubVersion],
    schemaKey: SchemaKey
  ): Boolean =
    (schemaKey.vendor === te.vendor) &&
      (schemaKey.name === te.schemaName) &&
      (schemaKey.version.model === te.model) &&
      subVersions.exists { case (revision, addition) =>
        schemaKey.version.revision === revision && schemaKey.version.addition === addition
      }

  private def castErrorToLoaderIgluError(
    schemaKey: SchemaKey,
    castErrors: NonEmptyList[CastError]
  ): NonEmptyList[FailureDetails.LoaderIgluError] =
    castErrors.map {
      case CastError.WrongType(v, e)      => FailureDetails.LoaderIgluError.WrongType(schemaKey, v, e.toString)
      case CastError.MissingInValue(k, v) => FailureDetails.LoaderIgluError.MissingInValue(schemaKey, k, v)
    }

  private def jsonForContext(sdd: SelfDescribingData[Json]): Json =
    sdd.data.mapObject { obj =>
      // Our special key takes priority over a key of the same name in the object
      obj.add("_schema_version", Json.fromString(sdd.schema.version.asString))
    }

  /**
   * Although this code looks prone to error, it is a very efficient way to build a Row from an
   * Event
   *
   * It is much more cpu-efficient than going via intermediate Json.
   *
   * TODO: implement this using Shapeless to make it less fragile
   */
  private def forAtomic[A](caster: Caster[A], event: Event): ValidatedNel[FailureDetails.LoaderIgluError, List[Caster.NamedValue[A]]] =
    (
      event.tr_total.traverse(forMoney(caster, _)),
      event.tr_tax.traverse(forMoney(caster, _)),
      event.tr_shipping.traverse(forMoney(caster, _)),
      event.ti_price.traverse(forMoney(caster, _)),
      event.tr_total_base.traverse(forMoney(caster, _)),
      event.tr_tax_base.traverse(forMoney(caster, _)),
      event.tr_shipping_base.traverse(forMoney(caster, _)),
      event.ti_price_base.traverse(forMoney(caster, _))
    ).mapN { case (trTotal, trTax, trShipping, tiPrice, trTotalBase, trTaxBase, trShippingBase, tiPriceBase) =>
      List[A](
        event.app_id.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.platform.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.etl_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_)),
        caster.timestampValue(event.collector_tstamp),
        event.dvce_created_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_)),
        event.event.fold[A](caster.nullValue)(caster.stringValue(_)),
        caster.stringValue(event.event_id.toString),
        event.txn_id.fold[A](caster.nullValue)(caster.intValue(_)),
        event.name_tracker.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.v_tracker.fold[A](caster.nullValue)(caster.stringValue(_)),
        caster.stringValue(event.v_collector),
        caster.stringValue(event.v_etl),
        event.user_id.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.user_ipaddress.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.user_fingerprint.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.domain_userid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.domain_sessionidx.fold[A](caster.nullValue)(caster.intValue(_)),
        event.network_userid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_country.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_region.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_city.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_zipcode.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_latitude.fold[A](caster.nullValue)(caster.doubleValue(_)),
        event.geo_longitude.fold[A](caster.nullValue)(caster.doubleValue(_)),
        event.geo_region_name.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ip_isp.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ip_organization.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ip_domain.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ip_netspeed.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_url.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_title.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_referrer.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_urlscheme.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_urlhost.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_urlport.fold[A](caster.nullValue)(caster.intValue(_)),
        event.page_urlpath.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_urlquery.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.page_urlfragment.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_urlscheme.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_urlhost.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_urlport.fold[A](caster.nullValue)(caster.intValue(_)),
        event.refr_urlpath.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_urlquery.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_urlfragment.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_medium.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_source.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_term.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_medium.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_source.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_term.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_content.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_campaign.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.se_category.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.se_action.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.se_label.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.se_property.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.se_value.fold[A](caster.nullValue)(caster.doubleValue(_)),
        event.tr_orderid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.tr_affiliation.fold[A](caster.nullValue)(caster.stringValue(_)),
        trTotal.getOrElse(caster.nullValue),
        trTax.getOrElse(caster.nullValue),
        trShipping.getOrElse(caster.nullValue),
        event.tr_city.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.tr_state.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.tr_country.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ti_orderid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ti_sku.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ti_name.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.ti_category.fold[A](caster.nullValue)(caster.stringValue(_)),
        tiPrice.getOrElse(caster.nullValue),
        event.ti_quantity.fold[A](caster.nullValue)(caster.intValue(_)),
        event.pp_xoffset_min.fold[A](caster.nullValue)(caster.intValue(_)),
        event.pp_xoffset_max.fold[A](caster.nullValue)(caster.intValue(_)),
        event.pp_yoffset_min.fold[A](caster.nullValue)(caster.intValue(_)),
        event.pp_yoffset_max.fold[A](caster.nullValue)(caster.intValue(_)),
        event.useragent.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_name.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_family.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_version.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_type.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_renderengine.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_lang.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_features_pdf.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_flash.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_java.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_director.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_quicktime.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_realplayer.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_windowsmedia.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_gears.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_features_silverlight.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_cookies.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.br_colordepth.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.br_viewwidth.fold[A](caster.nullValue)(caster.intValue(_)),
        event.br_viewheight.fold[A](caster.nullValue)(caster.intValue(_)),
        event.os_name.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.os_family.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.os_manufacturer.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.os_timezone.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.dvce_type.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.dvce_ismobile.fold[A](caster.nullValue)(caster.booleanValue(_)),
        event.dvce_screenwidth.fold[A](caster.nullValue)(caster.intValue(_)),
        event.dvce_screenheight.fold[A](caster.nullValue)(caster.intValue(_)),
        event.doc_charset.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.doc_width.fold[A](caster.nullValue)(caster.intValue(_)),
        event.doc_height.fold[A](caster.nullValue)(caster.intValue(_)),
        event.tr_currency.fold[A](caster.nullValue)(caster.stringValue(_)),
        trTotalBase.getOrElse(caster.nullValue),
        trTaxBase.getOrElse(caster.nullValue),
        trShippingBase.getOrElse(caster.nullValue),
        event.ti_currency.fold[A](caster.nullValue)(caster.stringValue(_)),
        tiPriceBase.getOrElse(caster.nullValue),
        event.base_currency.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.geo_timezone.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_clickid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.mkt_network.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.etl_tags.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.dvce_sent_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_)),
        event.refr_domain_userid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.refr_dvce_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_)),
        event.domain_sessionid.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.derived_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_)),
        event.event_vendor.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.event_name.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.event_format.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.event_version.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.event_fingerprint.fold[A](caster.nullValue)(caster.stringValue(_)),
        event.true_tstamp.fold[A](caster.nullValue)(caster.timestampValue(_))
      )
        .zip(AtomicFields.static)
        .map { case (value, field) =>
          Caster.NamedValue(field.name, value)
        }
    }.leftMap(castErrorToLoaderIgluError(AtomicFields.schemaKey, _))

  private val monetaryPrecision: Int = Type.DecimalPrecision.toInt(AtomicFields.monetaryDecimal.precision)

  private def forMoney[A](caster: Caster[A], d: Double): ValidatedNel[CastError, A] = {
    val bigDec = BigDecimal(d)
    Either.catchOnly[java.lang.ArithmeticException] {
      bigDec.setScale(AtomicFields.monetaryDecimal.scale, BigDecimal.RoundingMode.UNNECESSARY)
    } match {
      case Right(scaled) if scaled.precision <= monetaryPrecision =>
        caster.decimalValue(scaled.underlying.unscaledValue, AtomicFields.monetaryDecimal).valid
      case _ =>
        CastError.WrongType(Json.fromDoubleOrNull(d), AtomicFields.monetaryDecimal).invalidNel
    }
  }

}
