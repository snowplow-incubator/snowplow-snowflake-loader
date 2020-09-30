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
package com.snowplowanalytics.snowflake.loader.ast

import CopyInto._

case class CopyInto(
  schema: String,
  table: String,
  columns: List[String],
  from: From,
  credentials: Option[Auth],
  fileFormat: FileFormat,
  onError: Option[OnError],
  stripNullValues: Boolean) {     // Valid only for JSON
  /** Transform into statement, hiding all secrets */
  def sanitized: String = {
    val secrets = credentials match {
      case None =>
        Nil
      case Some(Auth.AwsKeys(id, key, token)) =>
        List(id, key) ++ token.toList
      case Some(Auth.AwsRole(roleArn)) =>
        List(roleArn)
      case Some(Auth.StorageIntegration(name)) =>
        List(name)
    }
    secrets.foldLeft(this.getStatement.value) { (result, s) => result.replace(s, "*") }
  }
}

object CopyInto {
  case class From(schema: String, stageName: String, path: String)
  case class FileFormat(schema: String, formatName: String)

  sealed trait OnError
  case object Continue extends OnError
  case object SkipFile extends OnError
  case class SkipFileNum(value: Int) extends OnError
  case class SkipFilePercentage(value: Int) extends OnError
  case object AbortStatement extends OnError
}
