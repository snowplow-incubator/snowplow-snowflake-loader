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

import java.util.{ Map => JMap }
import java.util.concurrent.TimeUnit
import java.time.{ Instant, ZoneId }

import scala.jdk.CollectionConverters._

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{AWSStaticCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.model._
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

import cats.implicits._
import cats.effect.{ Sync, Clock }
import cats.effect.concurrent.Ref

import fs2.Stream

import com.snowplowanalytics.snowflake.core.Config.S3Folder
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

/**
  * Entity responsible for getting all information about (un)processed folders
  */
trait ProcessManifest[F[_]] {
  // Loader-specific functions
  def markLoaded(tableName: String, runId: String): F[Unit]
  def scan(tableName: String): Stream[F, RunId]

  // Transformer-specific functions
  def add(tableName: String, runId: String): F[Unit]
  def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): F[Unit]
  def getUnprocessed(manifestTable: String, enrichedInput: S3Folder): F[List[String]]
}

/**
 * Helper module for working with process manifest
 */
object ProcessManifest {

  def apply[F[_]](implicit ev: ProcessManifest[F]): ProcessManifest[F] = ev

  type DbItem = JMap[String, AttributeValue]

  final case class Environment(s3: AmazonS3,
                               dynamoDb: AmazonDynamoDB,
                               attempts: Int,
                               awsRegion: String)

  type AppState[F[_]] = Ref[F, Environment]

  def getClient[F[_]: Sync](current: AppState[F], reinitialize: Boolean): F[Either[String, AmazonDynamoDB]] =
    current.get.flatMap {
      case Environment(_, _, attempts, _) if attempts > 3 =>
        Sync[F].pure(s"DynamoDB token expired $attempts times".asLeft)
      case Environment(s3, _, attempts, region) if reinitialize =>
        buildClient(region)
          .flatMap(client => current.set(Environment(s3, client, attempts + 1, region)).as(client.asRight))
      case Environment(_, client, _, _) =>
        Sync[F].pure(client.asRight)
    }

  def initState[F[_]: Sync](awsRegion: String): F[AppState[F]] =
    for {
      s3 <- getS3(awsRegion)
      dynamoDb <- buildClient(awsRegion)
      state <- Ref.of(Environment(s3, dynamoDb, 0, awsRegion))
    } yield state

  def buildClient[F[_]: Sync](awsRegion: String): F[AmazonDynamoDB] = Sync[F].delay {
    val credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials
    val provider = new AWSStaticCredentialsProvider(credentials)
    AmazonDynamoDBClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
  }

  /** Run a DynamoDB query; recreate the client on expired token exception */
  def runDynamoDbQuery[F[_]: Sync, T](current: AppState[F], query: AmazonDynamoDB => F[T]): F[T] =
    getClient(current, false).flatMap {
      case Right(dynamoDb) => query(dynamoDb).attempt.flatMap {
        case Right(t) => Sync[F].pure(t)
        case Left(e: AmazonServiceException) if e.getMessage.contains("The security token included in the request is expired")  =>
          for {
            client <- getClient(current, true)
            result <- client match {
              case Right(c) => query(c)
              case Left(e) => Sync[F].raiseError[T](new RuntimeException(e)) // Don't try to catch right after re-initialization
            }
          } yield result
        case Left(e) => Sync[F].raiseError(e)
      }
      case Left(e) => Sync[F].raiseError(new RuntimeException(e))
    }

  /** Get S3 client */
  def getS3[F[_]: Sync](awsRegion: String): F[AmazonS3] = Sync[F].delay {
    val credentials = DefaultAWSCredentialsProviderChain.getInstance().getCredentials
    val provider = new AWSStaticCredentialsProvider(credentials)
    AmazonS3ClientBuilder.standard().withRegion(awsRegion).withCredentials(provider).build()
  }

  def awsSyncProcessManifest[F[_]: Sync: Clock](state: AppState[F]): ProcessManifest[F] =
    new AwsManifest[F](state)

  /**
    * An instance for --dry-run, not doing any destructive changes
    * Not overwriting any Transformer-specific actions since --dry-run is used only in loader
    */
  def awsSyncDryProcessManifest[F[_]: Sync: Clock](state: AppState[F]): ProcessManifest[F] =
    new AwsManifest[F](state) {
      override def markLoaded(tableName: String, runId: String): F[Unit] =
        Sync[F].delay(println(s"Marking runid [$runId] processed (dry run)"))

      override def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): F[Unit] =
        Sync[F].raiseError(new IllegalStateException(s"Calling destructive method in DryRun (table $tableName, runId: $runId)"))

      override def add(tableName: String, runId: String): F[Unit] =
        Sync[F].raiseError(new IllegalStateException(s"Calling destructive add method in DryRun (table $tableName, runId: $runId)"))
    }

  /** Common implementation */
  private class AwsManifest[F[_]: Sync: Clock](state: AppState[F]) extends ProcessManifest[F] {
    def markLoaded(tableName: String, runId: String): F[Unit] =
      for {
        now <- getUtcSeconds
        request = new UpdateItemRequest()
          .withTableName(tableName)
          .withKey(Map(RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)).asJava)
          .withAttributeUpdates(Map(
            "LoadedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
            "LoadedBy" -> new AttributeValueUpdate().withValue(new AttributeValue(ProjectMetadata.version))
          ).asJava)
        query = (client: AmazonDynamoDB) => Sync[F].delay(client.updateItem(request)).void
        _ <- runDynamoDbQuery[F, Unit](state, query)
      } yield ()

    def scan(tableName: String): Stream[F, RunId] = {
      def getRequest =
        Sync[F].delay(new ScanRequest().withTableName(tableName))

      def runRequest =
        for {
          client <- getClient(state, false).flatMap {
            case Right(c) => Sync[F].pure(c)
            case Left(e) => Sync[F].raiseError[AmazonDynamoDB](new RuntimeException(e))
          }
          request <- getRequest
          result <- Sync[F].delay(client.scan(request))
        } yield result

      def runStream(run: F[ScanResult]): Stream[F, ScanResult] =
        Stream.eval(run).flatMap { result => Stream.emit(result) ++ continue(result) }

      def continue(last: ScanResult): Stream[F, ScanResult] =
        Option(last.getLastEvaluatedKey) match {
          case Some(key) =>
            val scanResult = for {
              request <- getRequest.map(_.withExclusiveStartKey(key))
              query = (client: AmazonDynamoDB) => Sync[F].delay(client.scan(request))
              result <- runDynamoDbQuery(state, query)
            } yield result
            runStream(scanResult)
          case None =>
            Stream.empty
        }

      runStream(runRequest)
        .flatMap { result => Stream.emits(result.getItems.asScala.toList) }
        .flatMap { item => RunId.parse(item.asScala) match {
          case Right(runId) => Stream.emit(runId)
          case Left(error) => Stream.raiseError[F](new RuntimeException(error))
        }}
    }

    def add(tableName: String, runId: String): F[Unit] =
      for {
        now <- getUtcSeconds
        request = new PutItemRequest()
          .withTableName(tableName)
          .withItem(Map(
            RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId),
            "AddedAt" -> new AttributeValue().withN(now.toString),
            "AddedBy" -> new AttributeValue(ProjectMetadata.version),
            "ToSkip" -> new AttributeValue().withBOOL(false)
          ).asJava)
        query = (client: AmazonDynamoDB) => Sync[F].delay(client.putItem(request)).void
        _ <- runDynamoDbQuery[F, Unit](state, query)
      } yield ()

    def markProcessed(tableName: String, runId: String, shredTypes: List[String], outputPath: String): F[Unit] =
      for {
        now <- getUtcSeconds
        shredTypesDynamo = shredTypes.map(t => new AttributeValue(t)).asJava
        request = new UpdateItemRequest()
          .withTableName(tableName)
          .withKey(Map(
            RunManifests.DynamoDbRunIdAttribute -> new AttributeValue(runId)
          ).asJava)
          .withAttributeUpdates(Map(
            "ProcessedAt" -> new AttributeValueUpdate().withValue(new AttributeValue().withN(now.toString)),
            "ShredTypes" -> new AttributeValueUpdate().withValue(new AttributeValue().withL(shredTypesDynamo)),
            "SavedTo" -> new AttributeValueUpdate().withValue(new AttributeValue(Config.fixPrefix(outputPath)))
          ).asJava)
        query = (client: AmazonDynamoDB) => Sync[F].delay(client.updateItem(request)).void
        _ <- runDynamoDbQuery[F, Unit](state, query)
      } yield ()

    def getUnprocessed(manifestTable: String, enrichedInput: S3Folder): F[List[String]] =
      for {
        s3 <- state.get.map(_.s3)
        allRuns <- Sync[F].delay(RunManifests.listRunIds(s3, enrichedInput.path))
        result <- scan(manifestTable).compile.fold(allRuns.toSet) { (s3Runs, manifestRun) =>
          s3Runs - manifestRun.runId
        }
      } yield result.toList

    // Conversions should not be necessary as realTime returns TZ-independent timestamp
    private def getUtcSeconds: F[Int] =
      Clock[F].realTime(TimeUnit.MILLISECONDS)
        .map(Instant.ofEpochMilli)
        .map(_.atZone(ZoneId.of("UTC")))
        .map(_.toInstant.toEpochMilli / 1000)
        .map(_.toInt)

  }
}
