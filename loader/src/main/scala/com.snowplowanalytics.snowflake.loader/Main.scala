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

import  com.snowplowanalytics.snowflake.core.Config

object Main {
  def main(args: Array[String]): Unit = {
    Config.parseLoaderCli(args) match {
      case Some(Right(config @ Config.CliLoaderConfiguration(Config.LoadCommand, _, _, _, _))) =>
        println("Loading...")
        Loader.run(config)
      case Some(Right(config @ Config.CliLoaderConfiguration(Config.SetupCommand, _, _, skip, _))) =>
        println("Setting up...")
        Initializer.run(config.loaderConfig, skip)
      case Some(Right(config @ Config.CliLoaderConfiguration(Config.MigrateCommand, _, loaderVersion, _, _))) =>
        println("Migrating...")
        Migrator.run(config.loaderConfig, loaderVersion)
      case Some(Left(error)) =>
        println(error)
        sys.exit(1)
      case None =>
        sys.exit(1)
    }
  }
}
