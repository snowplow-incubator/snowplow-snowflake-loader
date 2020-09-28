/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowflake.core

import scala.jdk.CollectionConverters._

import cats.data.{Validated, ValidatedNel}
import cats.implicits._

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import org.joda.time.{ DateTime, DateTimeZone }
import com.snowplowanalytics.snowflake.core.Config.S3Folder

/**
  * S3 folder saved in processing manifest. Added by Transformer job
  * Folders (run ids) refer to original archived folders in `enriched.archive`,
  * not folders produced by transformer
  */
sealed trait RunId extends Product with Serializable {
  /** S3 path to folder in **enriched archive** (without bucket name) */
  def runId: String
  /** Time when run id started to processing by Transformer job */
  def addedAt: DateTime
  /** Version of Snowflake transformer added an item. Meta-data */
  def addedBy: String
  /** Marker if run id should be skipped */
  def toSkip: Boolean
  /** Get name of folder with data (e.g. run=2017-08-10-03-02-01) */
  def runIdFolder: String =
    runId.split("/").lastOption.getOrElse(throw new RuntimeException(s"RunId [$runId] has invalid format"))
}

object RunId {

  /** Raw DynamoDB record */
  type RawItem = collection.Map[String, AttributeValue]

  /** Folder that just started to processing or marked to be skipped */
  case class FreshRunId(
      runId: String,
      addedAt: DateTime,
      addedBy: String,
      toSkip: Boolean)
    extends RunId

  /** Folder that has been processed, but not yet loaded */
  case class ProcessedRunId(
      runId: String,
      addedAt: DateTime,
      processedAt: DateTime,
      shredTypes: List[String],
      savedTo: S3Folder,
      addedBy: String,
      toSkip: Boolean)
    extends RunId

  /** Folder that has been processed and loaded */
  case class LoadedRunId(
      runId: String,
      addedAt: DateTime,
      processedAt: DateTime,
      shredTypes: List[String],
      savedTo: S3Folder,
      loadedAt: DateTime,
      addedBy: String,
      loadedBy: String)
    extends RunId {
    def toSkip = false
  }

  /**
    * Extract `RunId` from data returned from DynamoDB
    * @param rawItem DynamoDB item from processing manifest table
    * @return either error or one of possible run ids
    */
  def parse(rawItem: RawItem): Either[String, RunId] = {
    val runId = getRunId(rawItem)
    val startedAt = getStartedAt(rawItem)
    val processedAt = getProcessedAt(rawItem)
    val savedTo = getSavedTo(rawItem)
    val shredTypes = getShredTypes(rawItem)
    val loadedAt = getLoadedAt(rawItem)
    val addedBy = getAddedBy(rawItem)
    val loadedBy = getLoadedBy(rawItem)
    val toSkip = getToSkip(rawItem)

    val result = (runId, startedAt, processedAt, shredTypes, savedTo, loadedAt, addedBy, loadedBy, toSkip).mapN {
      case (run, started, None, None, None, None, added, None, skip) =>
        FreshRunId(run, started, added, skip).asRight
      case (run, started, Some(processed), Some(shredded), Some(saved), None, added, None, skip) =>
        ProcessedRunId(run, started, processed, shredded, saved, added, skip).asRight
      case (run, started, Some(processed), Some(shredded), Some(saved), Some(loaded), added, Some(loader), f) if !f =>  // LoadedRun cannot be skipped
        LoadedRunId(run, started, processed, shredded, saved, loaded, added, loader).asRight
      case (run, started, processed, shredded, saved, loaded, added, loader, skip) =>
        s"Invalid state: ${getStateMessage(run, started, processed, shredded, saved, loaded, added, loader, skip)}".asLeft
    }

    result match {
      case Validated.Valid(Right(success)) => success.asRight
      case Validated.Valid(Left(error)) => error.asLeft
      case Validated.Invalid(errors) =>
        s"Cannot extract RunId from DynamoDB record [$rawItem]. Errors: ${errors.toList.mkString(", ")}".asLeft
    }
  }

  private def getRunId(rawItem: RawItem): ValidatedNel[String, String] = {
    val runId = rawItem.get("RunId") match {
      case Some(value) if value.getS != null => Right(value.getS)
      case Some(_) => Left("Required RunId attribute has non-string type")
      case None => Left("Required RunId attribute is absent")
    }
    runId.toValidatedNel
  }

  private def getStartedAt(rawItem: RawItem) = {
    val startedAt = rawItem.get("AddedAt") match {
      case Some(value) if value.getN != null =>
        Right(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC))
      case Some(_) =>
        Left(s"Required AddedAt attribute has non-number type")
      case None =>
        Left(s"Required AddedAt attribute is absent")
    }
    startedAt.toValidatedNel
  }

  private def getProcessedAt(rawItem: RawItem) = {
    val processedAt = rawItem.get("ProcessedAt") match {
      case Some(value) if value.getN != null =>
        Right(Some(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC)))
      case Some(_) =>
        Left(s"Required ProcessedAt attribute has non-number type")
      case None =>
        Right(None)
    }
    processedAt.toValidatedNel
  }

  private def getShredTypes(rawItem: RawItem): ValidatedNel[String, Option[List[String]]] = {
    rawItem.get("ShredTypes") match {
      case Some(value) if value.getL == null =>
        s"Required ShredTypes attribute has non-list type in [$rawItem] record".invalidNel
      case Some(value) =>
        value.getL.asScala.toList.map { x =>
          val string = x.getS
          if (string != null)
            Validated.Valid(string)
          else
            Validated.Invalid("Invalid value in ShredTypes")
        }.sequence match {
          case Validated.Valid(list) => list.some.validNel
          case Validated.Invalid(errors) => errors.invalidNel[Option[List[String]]]
        }
      case None => none[List[String]].validNel
    }
  }

  private def getLoadedAt(rawItem: RawItem) = {
    val loadedAt = rawItem.get("LoadedAt") match {
      case Some(value) if value.getN != null =>
        Right(Some(new DateTime(value.getN.toLong * 1000L).withZone(DateTimeZone.UTC)))
      case Some(_) =>
        Left(s"Required LoadAt attribute has non-number type in [$rawItem] record")
      case None => Right(None)
    }
    loadedAt.toValidatedNel
  }

  private def getSavedTo(rawItem: RawItem) = {
    val savedTo = rawItem.get("SavedTo") match {
      case Some(value) if value.getS != null =>
        S3Folder.parse(value.getS).map(_.some)
      case Some(_) =>
        Left(s"Required SavedTo attribute has non-string type in [$rawItem] record")
      case None => Right(None)
    }
    savedTo.toValidatedNel
  }

  private def getAddedBy(rawItem: RawItem) = {
    val transformerVersion = rawItem.get("AddedBy") match {
      case Some(value) if value.getS != null => value.getS.asRight
      case Some(_) => "AddedBy attribute is present, but has non-string type".asLeft
      case None => "Required AddedBy attribute is absent".asLeft
    }
    transformerVersion.toValidatedNel
  }

  private def getLoadedBy(rawItem: RawItem) = {
    val loaderVersion = rawItem.get("LoadedBy") match {
      case Some(value) if value.getS != null => value.getS.some.asRight
      case Some(_) => "LoadedBy attribute is present, but has non-string type".asLeft
      case None => none[String].asRight
    }
    loaderVersion.toValidatedNel
  }

  private def getToSkip(rawItem: RawItem) = {
    val toSkip = rawItem.get("ToSkip") match {
      case Some(value) if value.getBOOL != null => value.getBOOL.asRight
      case Some(_) => "ToSkip attribute is present, but has non-bool type".asLeft
      case None => "Required ToSkip attribute is absent".asLeft
    }
    toSkip.toValidatedNel
  }

  def getStateMessage(
    runId: String,
    addedAt: DateTime,
    processedAt: Option[DateTime],
    shredTypes: Option[List[String]],
    savedTo: Option[S3Folder],
    loadedAt: Option[DateTime],
    addedBy: String,
    loadedBy: Option[String],
    toSkip: Boolean): String =
    s"RunId -> $runId, " +
      s"AddedAt -> $addedAt, " +
      s"ProcessedAt -> $processedAt, " +
      s"ShredTypes -> $shredTypes, " +
      s"SavedTo -> $savedTo, " +
      s"LoadedAt -> $loadedAt, " +
      s"AddedBy -> $addedBy, " +
      s"LoadedBy -> $loadedBy, " +
      s"ToSkip -> $toSkip"

}
