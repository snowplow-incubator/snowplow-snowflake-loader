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
package com.snowplowanalytics.snowflake.transformer.bad

import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec
import org.apache.spark.SparkException

class NotStoredBadRowsSpec extends TransformerJobSpec {
  private def runTransformerJobWithoutBadRows(lines: TransformerJobSpec.Lines): Unit =
    runTransformerJob(lines, badRowsShouldBeStored = false)

  override def appName = "enriched-event-with-invalid-timestamp"
  sequential
  "A job which processes Snowplow enriched events without outputing bad rows" should {

    "throw exception while trying to process enriched events with invalid timestamp" in {
      runTransformerJobWithoutBadRows(EnrichedEventWithInvalidTimestampSpec.lines) must throwA[SparkException]
    }

    "throw exception while trying to process event with invalid fields" in {
      runTransformerJobWithoutBadRows(InvalidEnrichedEventSpec.lines) must throwA[SparkException]
    }

    "throw exception while trying to process event with invalid jsons" in {
      runTransformerJobWithoutBadRows(InvalidJsonsSpec.lines) must throwA[SparkException]
    }

    "throw exception while trying to process not enriched event" in {
      runTransformerJobWithoutBadRows(NotEnrichedEventsSpec.lines) must throwA[SparkException]
    }
  }
}
