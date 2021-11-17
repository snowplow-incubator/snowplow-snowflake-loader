/*
 * Copyright (c) 2017-2021 Snowplow Analytics Ltd. All rights reserved.
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

import collection.mutable

import org.specs2.mutable.Specification

import com.amazonaws.services.dynamodbv2.model.AttributeValue

import org.joda.time.{ DateTime, DateTimeZone }

import com.snowplowanalytics.snowflake.core.RunId._
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{coerce => s3}

class RunIdSpec extends Specification {
  "Parse valid FreshRunId" in e2
  "Parse valid ProcessedRunId" in e3
  "Parse valid LoadedRunId" in e4
  "Parse valid RunId with unknown field" in e5
  "Fail to parse RunId without AddedAt" in e1
  "Fail to parse RunId with with ProcessedAt, but without ShredTypes" in e6
  "Fail to parse RunId with with invalid type" in e7

  def e2 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC), "some-transformer", false))
  }

  def e3 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01/"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "ToSkip" -> new AttributeValue().withBOOL(false)
    )

    val result = RunId.parse(input)
    result must beRight(ProcessedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "contexts_com_acme_context_1"),
      s3("s3://bucket/output/archived/run-01/"),
      "some-transformer",
      false)
    )
  }

  def e4 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "LoadedAt" -> new AttributeValue().withN("1502398136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "SavedTo" -> new AttributeValue("s3://bucket/output/archived/run-01"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "LoadedBy" ->  new AttributeValue("loader"),
      "ToSkip" -> new AttributeValue().withBOOL(false)
    )

    val result = RunId.parse(input)
    result must beRight(LoadedRunId(
      "enriched/archived/",
      new DateTime(1502357136000L).withZone(DateTimeZone.UTC),
      new DateTime(1502368136000L).withZone(DateTimeZone.UTC),
      List("unstruct_event_com_acme_event_1", "contexts_com_acme_context_1"),
      s3("s3://bucket/output/archived/run-01/"),
      new DateTime(1502398136000L).withZone(DateTimeZone.UTC),
      "some-transformer",
      "loader"))
  }

  def e5 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "AddedBy" ->  new AttributeValue("some-transformer"),
      "UnknownAttribute" -> new AttributeValue("something required by next version"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    result must beRight(FreshRunId("enriched/archived/", new DateTime(1502357136000L).withZone(DateTimeZone.UTC), "some-transformer", false))
  }

  def e1 = {
    val input = mutable.Map("RunId" -> new AttributeValue("enriched/archived/"))
    val result = RunId.parse(input)
    result must beLeft("Cannot extract RunId from DynamoDB record " +
      "[Map(RunId -> {S: enriched/archived/,})]. " +
      "Errors: Required AddedAt attribute is absent, Required AddedBy attribute is absent, Required ToSkip attribute is absent")
  }


  def e6 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withN("1502368136"),
      "AddedBy" ->  new AttributeValue("Transformer")
    )
    val result = RunId.parse(input)
    result must beLeft(
      "Cannot extract RunId from DynamoDB record " +
        "[Map(AddedBy -> {S: Transformer,}, AddedAt -> {N: 1502357136,}, RunId -> {S: enriched/archived/,}, ProcessedAt -> {N: 1502368136,})]. " +
        "Errors: Required ToSkip attribute is absent"
    )
  }

  def e7 = {
    val input = mutable.Map(
      "RunId" -> new AttributeValue("enriched/archived/"),
      "AddedAt" -> new AttributeValue().withN("1502357136"),
      "ProcessedAt" -> new AttributeValue().withS("1502368136"),
      "ShredTypes" -> new AttributeValue().withL(
        new AttributeValue("unstruct_event_com_acme_event_1"),
        new AttributeValue("contexts_com_acme_context_1")
      ),
      "AddedBy" -> new AttributeValue().withS("backfill-script"),
      "ToSkip" ->  new AttributeValue().withBOOL(false)
    )
    val result = RunId.parse(input)
    val expectedMessage = "Cannot extract RunId from DynamoDB record " +
      "[Map(ShredTypes -> {L: [{S: unstruct_event_com_acme_event_1,}, {S: contexts_com_acme_context_1,}],}, " +
      "ToSkip -> {BOOL: false}, AddedBy -> {S: backfill-script,}, AddedAt -> {N: 1502357136,}, " +
      "RunId -> {S: enriched/archived/,}, ProcessedAt -> {S: 1502368136,})]. " +
      "Errors: Required ProcessedAt attribute has non-number type"
    result must beLeft(expectedMessage)
  }
}
