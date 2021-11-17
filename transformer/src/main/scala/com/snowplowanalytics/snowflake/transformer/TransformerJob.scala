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
package com.snowplowanalytics.snowflake.transformer

import java.time.Instant

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import cats.syntax.foldable._
import cats.instances.list._
import cats.effect.IO

import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}
import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema

import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig
import com.snowplowanalytics.snowflake.core.ProcessManifest
import com.snowplowanalytics.snowflake.transformer.singleton.EventsManifestSingleton

object TransformerJob {

  private[transformer] val classesToRegister: Array[Class[_]] = Array(
    classOf[Array[String]],
    classOf[SchemaKey],
    classOf[SelfDescribingData[_]],
    classOf[Event],
    classOf[Instant],
    classOf[io.circe.Json],
    Class.forName("com.snowplowanalytics.iglu.core.SchemaVer$Full"),
    Class.forName("io.circe.JsonObject$LinkedHashMapJsonObject"),
    Class.forName("io.circe.Json$JObject"),
    Class.forName("io.circe.Json$JString"),
    Class.forName("io.circe.Json$JNull$"),
    Class.forName("io.circe.JsonLong"),
    Class.forName("io.circe.JsonDecimal"),
    Class.forName("io.circe.JsonBigDecimal"),
    Class.forName("io.circe.JsonBiggerDecimal"),
    Class.forName("io.circe.JsonDouble"),
    Class.forName("io.circe.JsonFloat"),
    classOf[org.apache.spark.sql.execution.datasources.WriteTaskResult],
    classOf[org.apache.spark.sql.execution.datasources.ExecutedWriteSummary],
    classOf[org.apache.spark.sql.execution.datasources.BasicWriteTaskStats],
    Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"),
    classOf[java.util.LinkedHashMap[_, _]],
    classOf[java.util.ArrayList[_]],
  )

  /** Process all directories, saving state into DynamoDB */
  def run(spark: SparkSession, manifest: ProcessManifest[IO], tableName: String, jobConfigs: List[TransformerJobConfig], eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema): IO[Unit] =
    jobConfigs.traverse_ { jobConfig =>
      for {
        _ <- IO(System.out.println(s"Snowflake Transformer: processing ${jobConfig.runId}. ${System.currentTimeMillis()}"))
        _ <- manifest.add(tableName, jobConfig.runId)
        shredTypes <- IO(process(spark, jobConfig, eventsManifestConfig, inbatch, atomicSchema))
        _ <- manifest.markProcessed(tableName, jobConfig.runId, shredTypes, jobConfig.goodOutput)
        _ <- IO(System.out.println(s"Snowflake Transformer: processed ${jobConfig.runId}. ${System.currentTimeMillis()}"))
      } yield ()
    }

  /**
    * Transform particular folder to Snowflake-compatible format and
    * return list of discovered shredded types
    *
    * @param spark                Spark SQL session
    * @param jobConfig            configuration with paths
    * @param eventsManifestConfig events manifest config instance
    * @param inbatch              whether inbatch deduplication should be used
    * @param atomicSchema         map of field names to maximum lengths
    * @return list of discovered shredded types
    */
  def process(spark: SparkSession, jobConfig: TransformerJobConfig, eventsManifestConfig: Option[EventsManifestConfig], inbatch: Boolean, atomicSchema: Schema) = {
    import spark.implicits._

    // Decide whether bad rows will be stored or not
    // If badOutput is supplied in the config, bad rows
    // need to be stored to given URL
    val storeBadRows: Boolean = jobConfig.badOutput.isDefined

    val sc = spark.sparkContext
    val keysAggregator = new StringSetAccumulator
    sc.register(keysAggregator)

    val inputRDD = sc
      .textFile(jobConfig.input)
      .map { line =>
        for {
          event <- Transformer.jsonify(line)
          sfErrChecked <- SnowflakeErrorCheck(event).toLeft(event)
          crossBatchDeduped <- Transformer.dedupeCrossBatch(sfErrChecked, EventsManifestSingleton.get(eventsManifestConfig))
            .map(t => if (t) Some(sfErrChecked) else None)
        } yield crossBatchDeduped
      }

    // Check if bad rows should be stored in case of error.
    // If bad rows need to be stored continue execution,
    // if bad rows not need to be stored, throw exception
    val withoutError = inputRDD.flatMap {
      case Right(Some(event)) => Some(event)
      case Right(None) => None
      case Left(_) if storeBadRows => None
      case Left(badRow) => throw new RuntimeException(s"Unhandled bad row ${badRow.compact}")
    }

    // Deduplicate the events in a batch if inbatch flagged set
    val inBatchDedupedEvents = if (inbatch) {
      withoutError
        .groupBy { e => (e.event_id, e.event_fingerprint) }
        .flatMap { case (_, vs) => vs.take(1) }
    } else withoutError

    val transformedEvents = inBatchDedupedEvents.map { e =>
      Transformer.transform(e, atomicSchema) match {
        case (keys, transformed) =>
          keysAggregator.add(keys)
          transformed
      }
    }

    // DataFrame is used only for S3OutputFormat
    transformedEvents
      .toDF
      .write
      .mode(SaveMode.Append)
      .text(jobConfig.goodOutput)

    jobConfig.badOutput match {
      case Some(badOutput) =>
        val withError = inputRDD
          .flatMap(_.swap.toOption)
          .map(e => Row(e.compact))
        spark.createDataFrame(withError, StructType(StructField("_", StringType, true) :: Nil))
          .write
          .mode(SaveMode.Overwrite)
          .text(badOutput)
      case _ => ()
    }

    val keysFinal = keysAggregator.value.toList
    keysAggregator.reset()
    keysFinal
  }
}
