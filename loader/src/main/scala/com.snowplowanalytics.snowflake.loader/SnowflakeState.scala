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

import cats.syntax.all._
import cats.effect.Sync

import fs2.Stream

import org.joda.time.DateTime

import com.snowplowanalytics.snowflake.core.RunId
import com.snowplowanalytics.snowflake.loader.SnowflakeState._

case class SnowflakeState(foldersToLoad: List[FolderToLoad]) extends AnyVal

object SnowflakeState {

  private val init: (List[RunId.ProcessedRunId], Set[String]) = (List.empty, Set.empty)

  def getState[F[_]: Sync](runIds: Stream[F, RunId]): F[SnowflakeState] = {
    val foldersAndTypes = runIds.compile.fold(init) { case ((folders, loadedTypes), id) =>
      if (id.toSkip) (folders, loadedTypes)
      else id match {
        case loaded: RunId.LoadedRunId => (folders, loaded.shredTypes.toSet ++ loadedTypes)
        case processed: RunId.ProcessedRunId => (processed :: folders, loadedTypes)
        case _ => (folders, loadedTypes)
      }
    }

    foldersAndTypes.map(foldState.tupled)
  }

  val foldState: (List[RunId.ProcessedRunId], Set[String]) => SnowflakeState = (folders, existingTypes) => {
    val init = List.empty[FolderToLoad]
    val foldersToLoad = folders.sortBy(_.addedAt).foldLeft(init) { case (folders, folder) =>
      val folderTypes = folder.shredTypes.toSet -- (folders.flatMap(_.folderToLoad.shredTypes).toSet ++ existingTypes)
      FolderToLoad(folder, folderTypes) :: folders
    }
    SnowflakeState(foldersToLoad.reverse)
  }

/**
  * Folder that was processed by Transformer and ready to be loaded into Snowflake,
  * and containing set of columns that first appeared in this folder (according to
  * manifest state)
  * @param folderToLoad reference to folder processed by Transformer
  * @param newColumns set of columns this processed folder brings
  */
  case class FolderToLoad(folderToLoad: RunId.ProcessedRunId, newColumns: Set[String])

  implicit def dateTimeOrdering: Ordering[DateTime] =
    Ordering.fromLessThan(_ isBefore _)
}
