/*
 * Copyright (c) 2017-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and
 * limitations there under.
 */
package com.snowplowanalytics.snowflake.transformer

import java.io.{BufferedWriter, File, FileWriter, IOException}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random

import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.TrueFileFilter
import org.apache.commons.io.filefilter.IOFileFilter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import cats.syntax.option._
import cats.syntax.either._

import com.snowplowanalytics.iglu.schemaddl.jsonschema.Schema
import com.snowplowanalytics.iglu.schemaddl.jsonschema.circe.implicits._
import com.snowplowanalytics.iglu.client.Resolver
import com.snowplowanalytics.snowflake.core.Cli.CompressionFormat
import com.snowplowanalytics.snowflake.core.{Cli, idClock}
import com.snowplowanalytics.snowflake.transformer.TransformerJobConfig.FSConfig
import com.snowplowanalytics.snowplow.eventsmanifest.EventsManifestConfig

import org.apache.spark.serializer.KryoSerializer

import org.specs2.mutable.Specification
import org.specs2.matcher.Matcher
import org.specs2.matcher.Matchers._
import org.specs2.specification.BeforeAfterAll

object TransformerJobSpec {
  /** Case class representing the input lines written in a file. */
  case class Lines(l: String*) {
    val lines = l.toList

    /** Write the lines to a file. */
    def writeTo(file: File): Unit = {
      val writer = new BufferedWriter(new FileWriter(file))
      for (line <- lines) {
        writer.write(line)
        writer.newLine()
      }
      writer.close()
    }

    def apply(i: Int): String = lines(i)
  }

  /** Case class representing the directories where the output of the job has been written. */
  case class OutputDirs(output: File, badRows: File) {
    /** Delete recursively the output and bad rows directories. */
    def deleteAll(): Unit = List(badRows, output).foreach(deleteRecursively)
  }

  /**
    * Read a part file at the given path into a List of Strings
    * @param root A root filepath
    * @param relativePath The relative path to the file from the root
    * @return the file contents as well as the file name
    */
  def readPartFile(root: File, relativePath: String): Option[(List[String], String)] = {
    val files = listFilesWithExclusions(new File(root, relativePath), List.empty)
      .filter(s => s.contains("part-"))
    def read(f: String): List[String] = Source.fromFile(new File(f)).getLines.toList
    files.foldLeft[Option[(List[String], String)]](None) { (acc, f) =>
      val accValue = acc.getOrElse((List.empty, ""))
      val contents = accValue._1 ++ read(f)
      Some((contents, f))
    }
  }

  /** Ignore empty files on output (necessary since https://github.com/snowplow/snowplow-rdb-loader/issues/142) */
  val NonEmpty = new IOFileFilter {
    def accept(file: File): Boolean = file.length() >= 1L
    def accept(dir: File, name: String): Boolean = true
  }

  /**
    * Recursively list files in a given path, excluding the supplied paths.
    * @param root A root filepath
    * @param exclusions A list of paths to exclude from the listing
    * @return the list of files contained in the root, minus the exclusions
    */
  def listFilesWithExclusions(root: File, exclusions: List[String]): List[String] =
    FileUtils.listFiles(root, NonEmpty, TrueFileFilter.TRUE)
      .asScala
      .toList
      .map(_.getCanonicalPath)
      .filter(p => !exclusions.contains(p) && !p.contains("crc") && !p.contains("SUCCESS"))
  /** A Specs2 matcher to check if a directory on disk is empty or not. */
  val beEmptyDir: Matcher[File] =
    ((f: File) =>
      !f.isDirectory ||
        f.list().length == 0 ||
        f.listFiles().filter(f => f.getName != "_SUCCESS" && !f.getName.endsWith(".crc")).map(_.length).sum == 0,
      "is populated dir")

  /**
    * Delete a file or directory and its contents recursively.
    * Throws an exception if deletion is unsuccessful.
    */
  def deleteRecursively(file: File): Unit = {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) throw new IOException(s"Failed to list files for dir: $file")
        files
      } else {
        Seq.empty[File]
      }
    }

    try {
      if (file.isDirectory) {
        var savedIOException: IOException = null
        for (child <- listFilesSafely(file)) {
          try {
            deleteRecursively(child)
          } catch {
            // In case of multiple exceptions, only last one will be thrown
            case ioe: IOException => savedIOException = ioe
          }
        }
        if (savedIOException != null) throw savedIOException
      }
    } finally {
      if (!file.delete()) {
        // Delete can also fail if the file simply did not exist
        if (file.exists()) throw new IOException(s"Failed to delete: ${file.getAbsolutePath}")
      }
    }
  }

  /**
    * Make a temporary file optionally filling it with contents.
    * @param tag an identifier who will become part of the file name
    * @param createParents whether or not to create the parent directories
    * @param containing the optional contents
    * @return the created file
    */
  def mkTmpFile(
                 tag: String,
                 createParents: Boolean = false,
                 containing: Option[Lines] = None
               ): File = {
    val f = File.createTempFile(s"snowplow-shred-job-${tag}-", "")
    if (createParents) f.mkdirs() else f.mkdir()
    containing.foreach(_.writeTo(f))
    f
  }

  /**
    * Create a file with the specified name with a random number at the end.
    * @param tag an identifier who will become part of the file name
    * @return the created file
    */
  def randomFile(tag: String): File =
    new File(System.getProperty("java.io.tmpdir"),
      s"snowplow-shred-job-${tag}-${Random.nextInt(Int.MaxValue)}")

  val resolverBase64 = "eyJzY2hlbWEiOiJpZ2x1OmNvbS5zbm93cGxvd2FuYWx5dGljcy5pZ2x1L3Jlc29sdmVyLWNvbmZpZy9qc29uc2NoZW1hLzEtMC0xIiwiZGF0YSI6eyJjYWNoZVNpemUiOjUsInJlcG9zaXRvcmllcyI6W3sibmFtZSI6IklnbHUgQ2VudHJhbCBiYXNlNjQiLCJwcmlvcml0eSI6MCwidmVuZG9yUHJlZml4ZXMiOlsiY29tLnNub3dwbG93YW5hbHl0aWNzIl0sImNvbm5lY3Rpb24iOnsiaHR0cCI6eyJ1cmkiOiJodHRwOi8vaWdsdWNlbnRyYWwuY29tIn19fV19fQ=="

  val resolver = Cli
    .Base64Encoded
    .parse(resolverBase64)
    .flatMap(r => Resolver.parse(r.json).leftMap(_.toString))
    .valueOr(e => throw new RuntimeException(s"Cannot parse test Iglu Resolver $e"))

  // Get Atomic schema from Iglu
  val atomic = resolver.lookupSchema(Main.AtomicSchema) match {
    case Right(jsonSchema) => Schema.parse(jsonSchema) match {
      case Some(schema) => schema
      case None =>
        println("Atomic event schema was invalid")
        sys.exit(1)
    }
    case Left(error) =>
      println("Cannot get atomic event schema")
      println(error)
      sys.exit(1)
  }

  val dynamodbDuplicateStorageTable = "snowplow-integration-test-crossbatch-deduplication"
  val dynamodbDuplicateStorageRegion = "us-east-1"

  val duplicateStorageConfig = EventsManifestConfig.DynamoDb(
    None,
    "local",
    None,
    dynamodbDuplicateStorageRegion,
    dynamodbDuplicateStorageTable
  )
}

/** Trait to mix in in every spec for the transformer job. */
trait TransformerJobSpec extends Specification with BeforeAfterAll {
  import TransformerJobSpec._
  val dirs = OutputDirs(randomFile("output"), randomFile("bad-rows"))

  // local[1] means the tests will run locally on one thread
  val conf: SparkConf = new SparkConf()
    .setMaster("local[1]")
    .setAppName(appName)
    .set("spark.serializer", classOf[KryoSerializer].getName)
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(TransformerJob.classesToRegister)


  var spark: SparkSession =
    SparkSession.builder()
      .config(conf)
      .getOrCreate()

  def appName: String

  /**
    * Run the transformer job with the specified lines as input.
    * @param lines input lines
    */
  def runTransformerJob(lines: Lines, inBatchDedup: Boolean = false, crossBatchDedup: Boolean = false, badRowsShouldBeStored: Boolean = true): Unit = {
      val input = mkTmpFile("input", createParents = true, containing = lines.some)
      val badOutput = if (badRowsShouldBeStored) Some(dirs.badRows.toString) else None
      val eventManifestConfig = if (crossBatchDedup) Some(duplicateStorageConfig) else None
      val config = FSConfig(input.toString, dirs.output.toString, badOutput)
      TransformerJob.process(spark, config, eventManifestConfig, inBatchDedup, atomic, Some(CompressionFormat.None))
      deleteRecursively(input)
  }
  override def afterAll(): Unit = {
    dirs.deleteAll()
  }

  override def beforeAll(): Unit = {
    ()
  }
}
