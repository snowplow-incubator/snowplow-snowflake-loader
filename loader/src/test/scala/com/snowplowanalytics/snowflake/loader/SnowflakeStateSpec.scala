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
package com.snowplowanalytics.snowflake.loader

import scala.util.Random.shuffle

import cats.effect.IO

import fs2.Stream

import org.specs2.mutable.Specification

import org.joda.time.DateTime

import com.snowplowanalytics.snowflake.core.RunId._
import com.snowplowanalytics.snowflake.core.Config.S3Folder.{ coerce => s3 }
import com.snowplowanalytics.snowflake.loader.SnowflakeState.FolderToLoad

class SnowflakeStateSpec extends Specification {

  "getState" should {
    "fold and sort list of processed folders into SnowflakeState" in {
      val initTime = 1502357136000L
      val input = List(
        ProcessedRunId("enriched/archived/run-01", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-02", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-03", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), "transformer-2", false)
      )
      val inputStream = Stream.emits(shuffle(input)).covary[IO]

      val expectedFolders = input.map {
        case id @ ProcessedRunId("enriched/archived/run-01", _, _, types, _, _, _) => FolderToLoad(id, types.toSet)
        case id @ ProcessedRunId("enriched/archived/run-02", _, _, _, _, _, _) => FolderToLoad(id, Set.empty)
        case id @ ProcessedRunId("enriched/archived/run-03", _, _, _, _, _, _) => FolderToLoad(id, Set.empty)
      }
      val expected = SnowflakeState(expectedFolders)

      val result = SnowflakeState.getState[IO](inputStream).unsafeRunSync()
      result must beEqualTo(expected)
    }

    "fold and sort list of loaded folders into SnowflakeState" in {
      val initTime = 1502357136000L
      val input = List(
        LoadedRunId("enriched/archived/run-01", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), new DateTime(1502368136000L + 5000), "transformer-2", "loader-1"),
        LoadedRunId("enriched/archived/run-02", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), new DateTime(1502368136000L + 7000), "transformer-2", "loader-1"),
        LoadedRunId("enriched/archived/run-03", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), new DateTime(1502368136000L + 9000), "transformer-3", "loader-1")
      )
      val inputStream = Stream.emits(shuffle(input)).covary[IO]

      val expected = SnowflakeState(Nil)

      val result = SnowflakeState.getState[IO](inputStream).unsafeRunSync()
      result must beEqualTo(expected)
    }

    "fold and sort list of run ids into SnowflakeState" in {
      val initTime = 1502357136000L
      val processedInput = List(
        ProcessedRunId("enriched/archived/run-01", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-01/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-02", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-02/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-03", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-03/"), "transformer-1", false)
      )
      val loadedInput = List(
        LoadedRunId("enriched/archived/run-04", new DateTime(initTime + 1000), new DateTime(1502368136000L + 3000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-04/"), new DateTime(1502368136000L + 5000), "transformer-1", "loader-2"),
        LoadedRunId("enriched/archived/run-05", new DateTime(initTime + 2000), new DateTime(1502368136000L + 5000), List("unstruct_event_com_acme_event_1", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), new DateTime(1502368136000L + 7000), "transformer-1", "loader-2"),
        LoadedRunId("enriched/archived/run-06", new DateTime(initTime + 3000), new DateTime(1502368136000L + 6000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), new DateTime(1502368136000L + 9000), "transformer-1", "loader-2")
      )
      val freshInput = List(
        FreshRunId("enriched/archived/run-07", new DateTime(initTime + 100000), "transformer-1", false),
        FreshRunId("enriched/archived/run-08", new DateTime(initTime + 200000), "transformer-1", false)
      )
      val inputStream = Stream.emits(shuffle(loadedInput ++ processedInput ++ freshInput)).covary[IO]


      val expectedFolders = processedInput.map {
        case id @ ProcessedRunId("enriched/archived/run-01", _, _, _, _, _, _) => FolderToLoad(id, Set.empty)
        case id @ ProcessedRunId("enriched/archived/run-02", _, _, _, _, _, _) => FolderToLoad(id, Set.empty)
        case id @ ProcessedRunId("enriched/archived/run-03", _, _, _, _, _, _) => FolderToLoad(id, Set.empty)
      }
      val expected = SnowflakeState(expectedFolders)

      val result = SnowflakeState.getState[IO](inputStream).unsafeRunSync()
      result must beEqualTo(expected)
    }
  }

  "foldState" should {
    "extract list of folders to load without existing types" in {
      val initTime = 1502357136000L
      val processedInput = List(
        ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false)
      )

      val expected = List(
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
          Set("context_com_acme_context_6")
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
          Set("unstruct_event_com_acme_event_7", "context_com_acme_context_1")
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
          Set.empty
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false),
          Set("context_com_acme_context_10", "context_com_acme_context_11")
        )
      )

      SnowflakeState.foldState(processedInput, Set.empty) must beEqualTo(SnowflakeState(expected))
    }

    "extract list of folders to load with existing types" in {
      val initTime = 1502357136000L
      val processedInput = List(
        ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
        ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false)
      )

      val expected = List(
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-04", new DateTime(initTime + 10000), new DateTime(1502368136000L + 30000), List("context_com_acme_context_6"), s3("s3://transformer-output/run-04/"), "transformer-1", false),
          Set("context_com_acme_context_6")
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-05", new DateTime(initTime + 20000), new DateTime(1502368136000L + 50000), List("unstruct_event_com_acme_event_7", "context_com_acme_context_1"), s3("s3://transformer-output/run-05/"), "transformer-1", false),
          Set("unstruct_event_com_acme_event_7")
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-06", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_1"), s3("s3://transformer-output/run-06/"), "transformer-1", false),
          Set.empty
        ),
        FolderToLoad(
          ProcessedRunId("enriched/archived/run-07", new DateTime(initTime + 30000), new DateTime(1502368136000L + 60000), List("context_com_acme_context_10", "context_com_acme_context_11"), s3("s3://transformer-output/run-07/"), "transformer-1", false),
          Set("context_com_acme_context_10")
        )
      )

      SnowflakeState.foldState(processedInput, Set("context_com_acme_context_1", "context_com_acme_context_11")) must beEqualTo(SnowflakeState(expected))
    }
  }
}
