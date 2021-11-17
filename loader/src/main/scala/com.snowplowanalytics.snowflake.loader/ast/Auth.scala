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
package com.snowplowanalytics.snowflake.loader.ast

/**
 * Represents all authentication options supported by Snowflake
 */
sealed trait Auth

object Auth {

  /**
   * Represents a Storage Integration entity in Snowflake
   * The recommended way to setup authentication
   * @param name Name of the storage integration
   */
  final case class StorageIntegration(name: String) extends Auth

  /**
   * Represents a pair of IAM keys with an optional session token
   */
  final case class AwsKeys(awsAccessKeyId: String, awsSecretKey: String, sessionToken: Option[String]) extends Auth

  /**
   * Represents an IAM role
   */
  final case class AwsRole(roleArn: String) extends Auth
}
