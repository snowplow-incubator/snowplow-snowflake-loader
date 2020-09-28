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
package com.snowplowanalytics.snowflake.transformer.good

import cats.implicits._

import io.circe.literal._
import io.circe.parser._

import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec

object EventDeduplicationSpec {
  import TransformerJobSpec._

  // original duplicated event_id
  val originalUuid = "1799a90f-f570-4414-b91a-b0db8f39cc2c"

  // Natural duplicate with same event_id, same payload and fingerprint (as first), but different collector and derived timestamps. Should be removed by natural deduplication
  val lines = Lines(
    s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$originalUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	""",
    s"""blog	web	2016-11-27 08:46:40.000	2016-11-27 07:16:08.000	2016-11-27 07:16:07.333	page_view	$originalUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:07.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	bed9a39a0917874d2ff072033a6413d9	"""
  )

  val expected = json"""{
    "page_urlhost":"chuwy.me",
    "br_features_realplayer":false,
    "etl_tstamp":"2016-11-27T08:46:40Z",
    "dvce_ismobile":false,
    "geo_latitude":null,
    "refr_medium":"internal",
    "ti_orderid":null,
    "br_version":"54.0.2840.98",
    "base_currency":null,
    "v_collector":"clj-1.1.0-tom-0.2.0",
    "mkt_content":null,
    "collector_tstamp":"2016-11-27T07:16:07Z",
    "os_family":"Mac OS X",
    "ti_sku":null,
    "event_vendor":"com.snowplowanalytics.snowplow",
    "network_userid":"5beb1f92-d4fb-4020-905c-f659929c8ab5",
    "br_renderengine":"WEBKIT",
    "br_lang":null,
    "tr_affiliation":null,
    "ti_quantity":null,
    "ti_currency":null,
    "geo_country":null,
    "user_fingerprint":"531497290",
    "mkt_medium":null,
    "page_urlscheme":"http",
    "ti_category":null,
    "pp_yoffset_min":null,
    "br_features_quicktime":false,
    "event":"page_view",
    "refr_urlhost":"chuwy.me",
    "user_ipaddress":"185.124.153.x",
    "br_features_pdf":true,
    "page_referrer":"http://chuwy.me/",
    "doc_height":4315,
    "refr_urlscheme":"http",
    "geo_region":null,
    "geo_timezone":null,
    "page_urlfragment":null,
    "br_features_flash":true,
    "os_manufacturer":"Apple Inc.",
    "mkt_clickid":null,
    "ti_price":null,
    "br_colordepth":"24",
    "event_format":"jsonschema",
    "tr_total":null,
    "pp_xoffset_min":null,
    "doc_width":1280,
    "geo_zipcode":null,
    "br_family":"Chrome",
    "tr_currency":null,
    "useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36",
    "event_name":"page_view",
    "os_name":"Mac OS X",
    "page_urlpath":"/scala-blocks.html",
    "br_name":"Chrome",
    "ip_netspeed":null,
    "page_title":"Scala Code Blocks",
    "ip_organization":null,
    "dvce_created_tstamp":"2016-11-27T07:16:07.333Z",
    "br_features_gears":false,
    "dvce_type":"Computer",
    "dvce_sent_tstamp":"2016-11-27T07:16:07.340Z",
    "se_action":null,
    "br_features_director":false,
    "se_category":null,
    "ti_name":null,
    "user_id":null,
    "refr_urlquery":null,
    "true_tstamp":null,
    "geo_longitude":null,
    "mkt_term":null,
    "v_tracker":"js-2.7.0-rc2",
    "os_timezone":"Asia/Omsk",
    "br_type":"Browser",
    "br_features_windowsmedia":false,
    "event_version":"1-0-0",
    "dvce_screenwidth":1280,
    "se_label":null,
    "domain_sessionid":"395e4506-37a3-4074-8de2-d8c75fb17d4a",
    "domain_userid":"1f9b3980-6619-4d75-a6c9-8253c76c3bfb",
    "page_urlquery":null,
    "refr_term":null,
    "refr_dvce_tstamp":null,
    "name_tracker":"blogTracker",
    "tr_tax_base":null,
    "dvce_screenheight":800,
    "mkt_campaign":null,
    "refr_urlfragment":null,
    "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1":[
    {
      "useragentFamily":"Chrome",
      "useragentMajor":"54",
      "useragentMinor":"0",
      "useragentPatch":"2840",
      "useragentVersion":"Chrome 54.0.2840",
      "osFamily":"MacOS X",
      "osMajor":"10",
      "osMinor":"11",
      "osPatch":"6",
      "osPatchMinor":null,
      "osVersion":"Mac OS X 10.11.6",
      "deviceFamily":"Other"
    }
    ],
    "tr_shipping":null,
    "tr_shipping_base":null,
    "br_features_java":false,
    "br_viewwidth":1280,
    "geo_city":null,
    "br_viewheight":726,
    "refr_domain_userid":null,
    "br_features_silverlight":false,
    "ti_price_base":null,
    "tr_tax":null,
    "br_cookies":true,
    "tr_total_base":null,
    "refr_urlport":80,
    "derived_tstamp":"2016-11-27T07:16:06.993Z",
    "app_id":"blog",
    "ip_isp":null,
    "geo_region_name":null,
    "pp_yoffset_max":null,
    "ip_domain":null,
    "domain_sessionidx":18,
    "pp_xoffset_max":null,
    "mkt_source":null,
    "page_urlport":80,
    "se_property":null,
    "platform":"web",
    "event_id":$originalUuid,
    "refr_urlpath":"/",
    "mkt_network":null,
    "se_value":null,
    "page_url":"http://chuwy.me/scala-blocks.html",
    "etl_tags":null,
    "tr_orderid":null,
    "tr_state":null,
    "txn_id":null,
    "refr_source":null,
    "tr_country":null,
    "tr_city":null,
    "doc_charset":"UTF-8",
    "event_fingerprint":"bed9a39a0917874d2ff072033a6413d9",
    "v_etl":"hadoop-1.8.0-common-0.24.0"
  }"""

}

class EventDeduplicationSpec extends TransformerJobSpec {
  import EventDeduplicationSpec._
  import TransformerJobSpec._
  override def appName = "event-deduplicaiton"

  sequential
  "A job which is provided with a two events with same event_id" should {
    runTransformerJob(lines, true)

    "transform two duplicate enriched events and store only first of them" in {
      val Some((lines, _)) = readPartFile(dirs.output, "")
      lines.map(parse).sequence must beRight(List(expected))
    }

    "not write any bad row JSONs" in {
      dirs.badRows must beEmptyDir
    }
  }
}
