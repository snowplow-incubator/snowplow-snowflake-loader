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

import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormat => JodaFormat, DateTimeFormatter => JodaFormatter }

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials }
import com.amazonaws.services.dynamodbv2.{ AmazonDynamoDBClientBuilder, AmazonDynamoDB }
import com.amazonaws.services.dynamodbv2.model.{AttributeDefinition, CreateTableRequest, KeySchemaElement, KeyType, ProvisionedThroughput, ScalarAttributeType}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import java.time.{ZoneId, ZonedDateTime}
import java.time.format.DateTimeFormatter

import cats.syntax.flatMap._
import cats.syntax.apply._
import cats.syntax.functor._
import cats.syntax.either._
import cats.syntax.traverse._
import cats.instances.list._

import cats.effect.{ Sync, IO }
import cats.effect.concurrent.Ref

import com.snowplowanalytics.snowflake.generated.ProjectMetadata

import org.specs2.mutable.Specification
import org.scalacheck.Gen

class ProcessManifestSpec extends Specification {
  "scan" should {
    "return same RunIds that were inserted into a manifest table" in {
      val test = for {
        dynamoDb <- ProcessManifestSpec.initDynamo[IO]
        _ <- ProcessManifestSpec.createTable[IO](dynamoDb)
        _ <- IO(println("Table has been created"))
        state <- ProcessManifestSpec.initLocal[IO](dynamoDb)
        manifest = ProcessManifest.awsSyncProcessManifest[IO](state)
        itemsGen = Gen.listOfN(4000, ProcessManifestSpec.runIdGen).map(ProcessManifestSpec.distinctOn(_.runId))
        runIds <- IO(itemsGen.sample.getOrElse(throw new RuntimeException("Couldn't generate run ids")))
        amount <- runIds.traverse(ProcessManifestSpec.addRunId[IO](manifest)).map(_.length)
        _ <- IO(println(s"$amount of run ids inserted into a table"))
        scanned <- manifest.scan(ProcessManifestSpec.LocalTable).compile.toList

        cleanedUpScanned = scanned.map(ProcessManifestSpec.hideDates)
        cleanedUpGenerated = runIds.map(ProcessManifestSpec.hideDates)
      } yield cleanedUpScanned must containTheSameElementsAs(cleanedUpGenerated)

      test.unsafeRunSync()
    }
  }
}

object ProcessManifestSpec {

  import ProcessManifest._

  val LocalTable = "manifest"

  def initDynamo[F[_]: Sync] = Sync[F].delay {
    val credentials = new BasicAWSCredentials("fake", "fake")
    val provider = new AWSStaticCredentialsProvider(credentials)
    AmazonDynamoDBClientBuilder.standard()
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", "us-east-1"))
      .withCredentials(provider).build()
  }

  def initS3[F[_]: Sync] = Sync[F].delay {
    val credentials = new BasicAWSCredentials("fake", "fake")
    val provider = new AWSStaticCredentialsProvider(credentials)
    AmazonS3ClientBuilder.standard().withRegion("us-east-1").withCredentials(provider).build()
  }

  def initLocal[F[_]: Sync](dynamoDb: AmazonDynamoDB): F[AppState[F]] =
    for {
      s3 <- initS3[F]
      state <- Ref.of(Environment(s3, dynamoDb, 0, "us-east-1"))
    } yield state

  def addRunId[F[_]: Sync](manifest: ProcessManifest[F])(runId: RunId) =
    runId match {
      case RunId.FreshRunId(id, _, _, _) =>
        manifest.add(LocalTable, id)
      case RunId.ProcessedRunId(id, _, _, types, savedTo, _, _) =>
        manifest.add(LocalTable, id) *>
          manifest.markProcessed(LocalTable, id, types, savedTo.path)
      case RunId.LoadedRunId(id, _, _, types, savedTo, _, _, _) =>
        manifest.add(LocalTable, id) *>
          manifest.markProcessed(LocalTable, id, types, savedTo.path) *>
          manifest.markLoaded(LocalTable, id)
    }

  def hideDates(runId: RunId) =
    runId match {
      case r: RunId.FreshRunId =>
        r.copy(addedAt = addedAtDefault)
      case r: RunId.ProcessedRunId =>
        r.copy(addedAt = addedAtDefault, processedAt = addedAtDefault)
      case r: RunId.LoadedRunId =>
        r.copy(addedAt = addedAtDefault, processedAt = addedAtDefault, loadedAt = addedAtDefault)
    }

  def createTable[F[_]: Sync](client: AmazonDynamoDB) = {
    val runIdAttribute = new AttributeDefinition("RunId", ScalarAttributeType.S)
    val manifestsSchema = new KeySchemaElement("RunId", KeyType.HASH)
    val throughput = new ProvisionedThroughput(10L, 100L)
    val req = new CreateTableRequest()
      .withTableName(LocalTable)
      .withAttributeDefinitions(runIdAttribute)
      .withKeySchema(manifestsSchema)
      .withProvisionedThroughput(throughput)

    Sync[F].delay(client.createTable(req)).void
  }

  val formatter: DateTimeFormatter  =
    DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss")

  val jodaFormatter: JodaFormatter =
    JodaFormat.forPattern("yyyy-MM-dd-HH-mm-ss")

  val startDateGen = Gen
    .calendar
    .map(_.toInstant.atZone(ZoneId.of("UTC")))

  val shreddedType =
    for {
      kind <- Gen.oneOf(List("contexts", "unstruct_event"))
      topDomain <- Gen.oneOf(List("com", "org", "me", "co_uk", "us", "ru"))
      domain <- Gen.oneOf(List("snowplow", "snowplowanalytics", "acme", "chuwy"))
      name <- Gen.oneOf("ping", "page_view", "click", "purchase")
      model <- Gen.oneOf(1,2,3,4,5)
    } yield s"${kind}_${topDomain}_${domain}_${name}_$model"

  val freshRunIdGen =
    for {
      startDate <- startDateGen
      startDateStr = formatter.format(startDate)
      runId = s"s3://snowflake-test/stage/run=$startDateStr/"
      addedAt <- addedAtGen(startDate)
    } yield RunId.FreshRunId(runId, addedAt, ProjectMetadata.version, false)

  val processedRunIdGen =
    for {
      startDate <- startDateGen
      startDateStr = formatter.format(startDate)
      runId = s"s3://snowflake-test/archive/enriched/good/run=$startDateStr/"
      processedAt <- processedAtGen(startDate)
      addedAt <- addedAtGen(startDate)
      types <- Gen.listOf(shreddedType).map(_.distinct)
      savedTo = Config.S3Folder.coerce(s"s3://snowflake-test/stage/run=$startDateStr/")
    } yield RunId.ProcessedRunId(runId, processedAt, addedAt, types, savedTo, ProjectMetadata.version, false)

  val loadedRunIdGen =
    for {
      startDate <- startDateGen
      startDateStr = formatter.format(startDate)
      runId = s"s3://snowflake-test/archive/enriched/good/run=$startDateStr/"
      processedAt <- processedAtGen(startDate)
      addedAt <- addedAtGen(startDate)
      types <- Gen.listOf(shreddedType).map(_.distinct)
      savedTo = Config.S3Folder.coerce(s"s3://snowflake-test/stage/run=$startDateStr/")
      loadedAt <- loadedAtGen(startDate)
    } yield RunId.LoadedRunId(runId, addedAt, processedAt, types, savedTo, loadedAt, ProjectMetadata.version, ProjectMetadata.version)

  val runIdGen =
    Gen.oneOf(freshRunIdGen, processedRunIdGen, loadedRunIdGen)

  def addedAtGen(start: ZonedDateTime) =
    Gen.choose(2, 10)
      .flatMap(x => start.plusMinutes(x.toLong))
      .map(x => DateTime.parse(x.format(formatter), jodaFormatter))

  def processedAtGen(start: ZonedDateTime) =
    Gen.choose(10, 20)
      .flatMap(x => start.plusMinutes(x.toLong))
      .map(x => DateTime.parse(x.format(formatter), jodaFormatter))

  def loadedAtGen(start: ZonedDateTime) =
    Gen.choose(20, 25)
      .flatMap(x => start.plusMinutes(x.toLong))
      .map(x => DateTime.parse(x.format(formatter), jodaFormatter))

  val addedAtDefault =
    DateTime.now()

  /** We need to remove all elements with duplicated RunId */
  def distinctOn[A, B](f: A => B)(list: List[A]): List[A] = list.foldLeft(List.empty[A]) { (acc, elem) =>
    val value = f(elem)
    acc.find(item => f(item) == value) match {
      case Some(_) => acc         // if found, do not add but return accumelation
      case None    => elem :: acc // if not found, add elem to accumelation
    }
  }
}
