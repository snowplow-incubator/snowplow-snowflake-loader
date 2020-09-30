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

import cats.syntax.either._

import org.specs2.mutable.Specification

import scala.io.Source

class AtomicDefSpec extends Specification {
  "getTable" should {
    "CREATE atomic.events" in {
      val referenceStream = getClass.getResourceAsStream("/sql/atomic-def.sql")
      val expectedLines = Source.fromInputStream(referenceStream).getLines().toList
      val expected = List(AtomicDefSpec.normalizeSql(expectedLines).mkString(""))

      val resultLines = AtomicDef.getTable().getStatement.value.split("\n").toList
      val result = AtomicDefSpec.normalizeSql(resultLines)

      result must beEqualTo(expected)
    }
  }

  "compare" should {
    "not warn about new valid columns" in {
      val input = (AtomicDef.columns :+ Column("expected", SnowflakeDatatype.Variant)).map(_.asRight)
      AtomicDef.compare(input) must beEqualTo(Nil)
    }

    "warn about parsing failures" in {
      val input = AtomicDef.columns.map(_.asRight) :+ "Parsing failure".asLeft
      AtomicDef.compare(input) must beEqualTo(List("Could not parse column with 129 position. Parsing failure"))
    }

    "warn about unmatched columns" in {
      val input = AtomicDef.columns.map(_.asRight).patch(3, List(Column("unexpected", SnowflakeDatatype.Variant).asRight), 1)
      AtomicDef.compare(input) must beEqualTo(List("Existing column [unexpected VARIANT] doesn't match expected definition [collector_tstamp TIMESTAMP NOT NULL] at position 4"))
    }
  }
}


object AtomicDefSpec {
  /** Remove comments and formatting */
  def normalizeSql(lines: List[String]) = lines
    .map(_.dropWhile(_.isSpaceChar))
    .map(line => if (line.startsWith("--")) "" else line)
    .filterNot(_.isEmpty)
    .map(_.replaceAll("""\s+""", " "))
    .map(_.replaceAll(""",\s""", ","))
}
