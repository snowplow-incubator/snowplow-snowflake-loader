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

import java.io.File

import cats.implicits._

import io.circe.literal._
import io.circe.parser.parse

import com.snowplowanalytics.snowflake.generated.ProjectMetadata
import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec

object EnrichedEventWithInvalidTimestampSpec {
  import TransformerJobSpec._
  val lines = Lines(
    """snowplowweb	web	2014-06-01 14:04:11.639	-0043-03-12 18:16:35.000	2014-05-29 18:16:35.967	unstruct	2b1b25a4-c0df-4859-8201-cf21492ad61b	114221	clojure	js-2.0.0-M2	clj-0.6.0-tom-0.0.4	hadoop-0.5.0-common-0.4.0		68.42.204.218	1242058182	58df65c46e1ac937	11	437ad25b-2006-455e-b5d8-d664b74df8f3	US	MI	Holland	49423	42.742294	-86.0661						http://snowplowanalytics.com/blog/		https://www.google.com/	http	snowplowanalytics.com	80	/blog/			https	www.google.com	80	/			search	Google													{"schema":"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0","data":{"schema":"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0","data":{"targetUrl":"http://snowplowanalytics.com/blog/page2","elementClasses":["next"]}}}																			Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36	Chrome	Chrome		Browser	WEBKIT	en-US	1	1	1	0	1	0	0	0	1	1	24	1241	806	Mac OS	Mac OS	Apple Inc.	America/New_York	Computer	0	1440	900	UTF-8																									"""
  )

  val expected = json"""{
    "schema" : "iglu:com.snowplowanalytics.snowplow.badrows/generic_error/jsonschema/1-0-0",
    "data" : {
      "processor" : {
        "artifact" : "snowplow-snowflake-transformer",
        "version" : ${ProjectMetadata.version}
      },
      "failure" : {
        "timestamp" : $DefaultTimestamp,
        "errors" : [
          "Timestamp -0043-03-12T18:16:35Z is out of Snowflake range, at column collector_tstamp"
        ]
      },
      "payload" : "{\"app_id\":\"snowplowweb\",\"platform\":\"web\",\"etl_tstamp\":\"2014-06-01T14:04:11.639Z\",\"collector_tstamp\":\"-0043-03-12T18:16:35Z\",\"dvce_created_tstamp\":\"2014-05-29T18:16:35.967Z\",\"event\":\"unstruct\",\"event_id\":\"2b1b25a4-c0df-4859-8201-cf21492ad61b\",\"txn_id\":114221,\"name_tracker\":\"clojure\",\"v_tracker\":\"js-2.0.0-M2\",\"v_collector\":\"clj-0.6.0-tom-0.0.4\",\"v_etl\":\"hadoop-0.5.0-common-0.4.0\",\"user_id\":null,\"user_ipaddress\":\"68.42.204.218\",\"user_fingerprint\":\"1242058182\",\"domain_userid\":\"58df65c46e1ac937\",\"domain_sessionidx\":11,\"network_userid\":\"437ad25b-2006-455e-b5d8-d664b74df8f3\",\"geo_country\":\"US\",\"geo_region\":\"MI\",\"geo_city\":\"Holland\",\"geo_zipcode\":\"49423\",\"geo_latitude\":42.742294,\"geo_longitude\":-86.0661,\"geo_region_name\":null,\"ip_isp\":null,\"ip_organization\":null,\"ip_domain\":null,\"ip_netspeed\":null,\"page_url\":\"http://snowplowanalytics.com/blog/\",\"page_title\":null,\"page_referrer\":\"https://www.google.com/\",\"page_urlscheme\":\"http\",\"page_urlhost\":\"snowplowanalytics.com\",\"page_urlport\":80,\"page_urlpath\":\"/blog/\",\"page_urlquery\":null,\"page_urlfragment\":null,\"refr_urlscheme\":\"https\",\"refr_urlhost\":\"www.google.com\",\"refr_urlport\":80,\"refr_urlpath\":\"/\",\"refr_urlquery\":null,\"refr_urlfragment\":null,\"refr_medium\":\"search\",\"refr_source\":\"Google\",\"refr_term\":null,\"mkt_medium\":null,\"mkt_source\":null,\"mkt_term\":null,\"mkt_content\":null,\"mkt_campaign\":null,\"contexts\":{},\"se_category\":null,\"se_action\":null,\"se_label\":null,\"se_property\":null,\"se_value\":null,\"unstruct_event\":{\"schema\":\"iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0\",\"data\":{\"schema\":\"iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-0\",\"data\":{\"targetUrl\":\"http://snowplowanalytics.com/blog/page2\",\"elementClasses\":[\"next\"]}}},\"tr_orderid\":null,\"tr_affiliation\":null,\"tr_total\":null,\"tr_tax\":null,\"tr_shipping\":null,\"tr_city\":null,\"tr_state\":null,\"tr_country\":null,\"ti_orderid\":null,\"ti_sku\":null,\"ti_name\":null,\"ti_category\":null,\"ti_price\":null,\"ti_quantity\":null,\"pp_xoffset_min\":null,\"pp_xoffset_max\":null,\"pp_yoffset_min\":null,\"pp_yoffset_max\":null,\"useragent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.114 Safari/537.36\",\"br_name\":\"Chrome\",\"br_family\":\"Chrome\",\"br_version\":null,\"br_type\":\"Browser\",\"br_renderengine\":\"WEBKIT\",\"br_lang\":\"en-US\",\"br_features_pdf\":true,\"br_features_flash\":true,\"br_features_java\":true,\"br_features_director\":false,\"br_features_quicktime\":true,\"br_features_realplayer\":false,\"br_features_windowsmedia\":false,\"br_features_gears\":false,\"br_features_silverlight\":true,\"br_cookies\":true,\"br_colordepth\":\"24\",\"br_viewwidth\":1241,\"br_viewheight\":806,\"os_name\":\"Mac OS\",\"os_family\":\"Mac OS\",\"os_manufacturer\":\"Apple Inc.\",\"os_timezone\":\"America/New_York\",\"dvce_type\":\"Computer\",\"dvce_ismobile\":false,\"dvce_screenwidth\":1440,\"dvce_screenheight\":900,\"doc_charset\":\"UTF-8\",\"doc_width\":null,\"doc_height\":null,\"tr_currency\":null,\"tr_total_base\":null,\"tr_tax_base\":null,\"tr_shipping_base\":null,\"ti_currency\":null,\"ti_price_base\":null,\"base_currency\":null,\"geo_timezone\":null,\"mkt_clickid\":null,\"mkt_network\":null,\"etl_tags\":null,\"dvce_sent_tstamp\":null,\"refr_domain_userid\":null,\"refr_dvce_tstamp\":null,\"derived_contexts\":{},\"domain_sessionid\":null,\"derived_tstamp\":null,\"event_vendor\":null,\"event_name\":null,\"event_format\":null,\"event_version\":null,\"event_fingerprint\":null,\"true_tstamp\":null}"
      }
    }"""

}

class EnrichedEventWithInvalidTimestampSpec extends TransformerJobSpec {
  import TransformerJobSpec._
  override def appName = "enriched-event-with-invalid-timestamp"
  sequential
  "A job which processes Snowplow enriched events with invalid timestamp" should {
    runTransformerJob(EnrichedEventWithInvalidTimestampSpec.lines)

    "write a bad row JSON with enriched event and error message for each input line" in {
      val Some((jsons, _)) = readPartFile(dirs.badRows, "")
      jsons.map(parse).sequence.map(clearTimestamps) must beRight(List(EnrichedEventWithInvalidTimestampSpec.expected))
    }

    "not write any good events" in {
      new File(dirs.output, "") must beEmptyDir
    }

  }
}
