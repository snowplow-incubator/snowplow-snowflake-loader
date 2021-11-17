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

import com.snowplowanalytics.snowflake.loader.ast.CreateWarehouse._

case class CreateWarehouse(name: String, size: Option[Size], autoSuspend: Option[Int], autoResume: Option[Boolean])

object CreateWarehouse {

  val DefaultSize: Size = XSmall
  val DefaultAutoSuspend: Int = 300
  val DefaultAutoResume: Boolean = true

  sealed trait Size
  case object XSmall extends Size
  case object Small extends Size
  case object Medium extends Size
  case object Large extends Size
  case object XLarge extends Size
  case object XxLarge extends Size
  case object XxxLarge extends Size
}
