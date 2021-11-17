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
package com.snowplowanalytics.snowflake.transformer.bad

import java.io.File

import cats.implicits._

import io.circe.literal._
import io.circe.parser._

import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec
import com.snowplowanalytics.snowflake.generated.ProjectMetadata

object NotEnrichedEventsSpec {
  import TransformerJobSpec._
  val lines = Lines(
    "",
    "NOT AN ENRICHED EVENT",
    "2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net"
  )
  val expected = List(
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0",
      "data": {
        "payload":"",
        "failure": {
          "type" : "NotTSV"
        },
        "processor" : {
          "artifact" : "snowplow-snowflake-transformer",
          "version" : ${ProjectMetadata.version}
        }
      }
    }""",
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0",
      "data": {
        "payload":"NOT AN ENRICHED EVENT",
        "failure": {
          "type" : "NotTSV"
        },
        "processor" : {
          "artifact" : "snowplow-snowflake-transformer",
          "version" : ${ProjectMetadata.version}
        }
      }
    }""",
    json"""{
      "schema": "iglu:com.snowplowanalytics.snowplow.badrows/loader_parsing_error/jsonschema/2-0-0",
      "data": {
        "payload":"2012-05-21  07:14:47  FRA2  3343  83.4.209.35 GET d3t05xllj8hhgj.cloudfront.net",
        "failure": {
          "type" : "NotTSV"
        },
        "processor" : {
          "artifact" : "snowplow-snowflake-transformer",
          "version" : ${ProjectMetadata.version}
        }
      }
    }""")
}

class NotEnrichedEventsSpec extends TransformerJobSpec {
  import TransformerJobSpec._
  override def appName = "not-enriched-events"
  sequential
  "A job which processes input lines not containing Snowplow enriched events" should {
    runTransformerJob(NotEnrichedEventsSpec.lines)

    "write a bad row JSON with input line and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.map(parse).sequence must beRight(NotEnrichedEventsSpec.expected)
    }

    "not write any good event" in {
      new File(dirs.output, "") must beEmptyDir
    }
  }
}
