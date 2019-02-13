/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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

import ast._
import com.snowplowanalytics.snowflake.core.Config
import com.snowplowanalytics.snowflake.core.Config.SetupSteps
import connection.Jdbc

/** Module containing functions to setup Snowflake table for Enriched events */
object Initializer {

  /** Run setup process */
  def run(config: Config, skip: Set[SetupSteps]): Unit = {
    val connection = Jdbc.getConnection(config)

    // Save only static credentials
    val credentials = PasswordService.getSetupCredentials(config.auth)

    if (!skip.contains(SetupSteps.Schema)) {
      Jdbc.executeAndOutput(connection, CreateSchema(config.schema))
    }

    if (!skip.contains(SetupSteps.Table)) {
      Jdbc.executeAndOutput(connection, AtomicDef.getTable(config.schema))
    }

    if (!skip.contains(SetupSteps.Warehouse)) {
      Jdbc.executeAndOutput(connection, CreateWarehouse(config.warehouse, size = Some(CreateWarehouse.XSmall), autoSuspend = Some(300), autoResume = Some(true)))
    }

    if (!skip.contains(SetupSteps.FileFormat)) {
      Jdbc.executeAndOutput(connection, CreateFileFormat.CreateJsonFormat(Defaults.FileFormat))
    }

    if (!skip.contains(SetupSteps.Stage)) {
      Jdbc.executeAndOutput(connection, CreateStage(config.stage, config.stageUrl, Defaults.FileFormat, config.schema, credentials))
    }
    
    connection.close()
  }
}
