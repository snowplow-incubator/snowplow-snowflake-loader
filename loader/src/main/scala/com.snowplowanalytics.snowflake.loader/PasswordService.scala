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

import scala.util.control.NonFatal

import cats.syntax.either._

import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, Credentials, GetSessionTokenRequest}
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

import com.snowplowanalytics.snowflake.core.Config

import ast.Common


/** Security/auth-related functions */
object PasswordService {

  sealed trait CredentialsStatus
  case object NoCredentials extends CredentialsStatus
  case class CredentialsFailure(message: String) extends CredentialsStatus

  /** Get credentials **only** if they're provided explicitly, use for `setup` */
  def getSetupCredentials(authMethod: Config.AuthMethod): Option[Common.AwsCreds] =
    authMethod match {
      case Config.AuthMethod.CredentialsAuth(accessKeyId, secretAccessKey) =>
        Some(Common.AwsCreds(accessKeyId, secretAccessKey, None, None))
      case _ => None
    }

  /** Get credentials by trying all possible ways: explicit, temporary, provider chain */
  def getLoadCredentials(authMethod: Config.AuthMethod): Either[CredentialsStatus, Common.AwsCreds] =
    authMethod match {
      case Config.AuthMethod.CredentialsAuth(accessKeyId, secretAccessKey) =>
        Right(Common.AwsCreds(accessKeyId, secretAccessKey, None, None))
      case Config.AuthMethod.RoleAuth(roleArn, sessionDuration) =>
        getCredentialsForRole(roleArn, sessionDuration).map { creds =>
          Common.AwsCreds(creds.getAccessKeyId, creds.getSecretAccessKey, Option(creds.getSessionToken), Option(roleArn))
        }.leftMap(e => CredentialsFailure(e))
      case Config.AuthMethod.StageAuth => Left(NoCredentials)
    }

  /**
    * Get value from AWS EC2 Parameter Store
    * @param name systems manager parameter's name with SSH key
    * @return decrypted string with key
    */
  def getKey(name: String): Either[String, String] = {
    try {
      val client = AWSSimpleSystemsManagementClientBuilder.defaultClient()
      val req: GetParameterRequest = new GetParameterRequest().withName(name).withWithDecryption(true)
      val par = client.getParameter(req)
      Right(par.getParameter.getValue)
    } catch {
      case NonFatal(e) => Left(e.getMessage)
    }
  }

  /** Get temporary credentials for role. Unlike `getCredentialsForSession` can be constrained by role's policies */
  def getCredentialsForRole(roleArn: String, durationSec: Int): Either[String, Credentials] = {
    val client = AWSSecurityTokenServiceClientBuilder.standard().build()
    val request = new AssumeRoleRequest()
      .withRoleArn(roleArn)
      .withRoleSessionName(s"snowflake_${System.currentTimeMillis()}")
      .withDurationSeconds(durationSec)

    try {
      val sessionTokenResult = client.assumeRole(request)
      Right(sessionTokenResult.getCredentials)
    } catch {
      case NonFatal(e) =>
        Left(s"Cannot get temporary credentials for role. ${e.getMessage}")
    }
  }

  /** Get temporary credentials for sesion. Unlike `getCredentialsForRole` gives user's credentials */
  def getCredentialsForSession(durationSec: Int): Either[String, Credentials] = {
    val client = AWSSecurityTokenServiceClientBuilder.standard().build()
    val getSessionTokenRequest = new GetSessionTokenRequest()
      .withDurationSeconds(durationSec)

    try {
      val sessionTokenResult = client.getSessionToken(getSessionTokenRequest)
      Right(sessionTokenResult.getCredentials)
    } catch {
      case NonFatal(e) =>
        Left(s"Cannot get temporary credentials for session token. ${e.getMessage}")
    }
  }
}
