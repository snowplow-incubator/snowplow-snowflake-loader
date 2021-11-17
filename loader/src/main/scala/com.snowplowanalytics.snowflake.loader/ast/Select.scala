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

import Select._

/**
  * AST for SELECT statement
  * E.g. SELECT raw:app_id::VARCHAR, event_id:event_id::VARCHAR FROM temp_table
  * @param columns list of columns, casted to specific type
  * @param table source table name
  */
case class Select(columns: List[CastedColumn], schema: String, table: String)

object Select {
  case class CastedColumn(originColumn: String, columnName: String, datatype: SnowflakeDatatype, substring: Option[Substring] = None)
  case class Substring(start: Int, length: Int)
}
