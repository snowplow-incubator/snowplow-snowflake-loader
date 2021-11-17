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

import com.snowplowanalytics.snowflake.core.Config._

sealed trait TransformerJobConfig {
  def input: String
  def goodOutput: String
  def badOutput: Option[String]
  def runId: String
}

object TransformerJobConfig {

  final case class S3Config(enrichedArchive: S3Folder, snowflakeOutput: S3Folder, badOutputFolder: Option[S3Folder], s3a: Boolean, runId: String) extends TransformerJobConfig {
    val protocol = if (s3a) "s3a" else "s3"

    def input: String = {
      val (enrichedBucket, enrichedPath) = enrichedArchive.splitS3Folder
      s"$protocol://$enrichedBucket/$enrichedPath$runIdFolder/*"
    }

    def goodOutput: String = {
      val (bucket, path) = snowflakeOutput.splitS3Folder
      s"$protocol://$bucket/$path$runIdFolder"
    }

    def badOutput: Option[String] = {
      badOutputFolder.map { o =>
        val (bucket, path) = o.splitS3Folder
        s"$protocol://$bucket/$path$runIdFolder"
      }
    }

    def runIdFolder: String = runId.split("/").last
  }

  final case class FSConfig(input: String, goodOutput: String, badOutput: Option[String]) extends TransformerJobConfig {
    def runId: String = "fs-run-id"
  }
}
