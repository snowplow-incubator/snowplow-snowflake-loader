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
package com.snowplowanalytics.snowflake.transformer.good

// java
import java.time.Instant
import java.util.UUID

import com.snowplowanalytics.snowflake.transformer.Transformer

// circe
import io.circe.parser._
import io.circe.syntax._
import io.circe.{Json, JsonObject}

// specs2
import org.specs2.mutable.Specification

// scala-analytics-sdk
import com.snowplowanalytics.snowplow.analytics.scalasdk.Event
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}

// iglu
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

// This library
import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec

object TransformerSpec {
  val atomicSchema = TransformerJobSpec.atomic

  val atomic = atomicSchema.properties.map { properties => properties.value.mapValues { property =>
    property.maxLength.map {_.value.intValue}
  }}.getOrElse(throw new RuntimeException("Could not convert schema to map"))

  val event = Event(
    app_id = Some("angry-birds"),
    platform = Some("web"),
    etl_tstamp = Some(Instant.parse("2017-01-26T00:01:25.292Z")),
    collector_tstamp = Instant.parse("2013-11-26T00:02:05Z"),
    dvce_created_tstamp = Some(Instant.parse("2013-11-26T00:03:57.885Z")),
    event = Some("page_view"),
    event_id = UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
    txn_id = Some(41828),
    name_tracker = Some("cloudfront-1"),
    v_tracker = Some("js-2.1.0"),
    v_collector = "clj-tomcat-0.1.0",
    v_etl = "serde-0.5.2",
    user_id = Some("jon.doe@email.com"),
    user_ipaddress = Some("92.231.54.234"),
    user_fingerprint = Some("2161814971"),
    domain_userid = Some("bc2e92ec6c204a14"),
    domain_sessionidx = Some(3),
    network_userid = Some("ecdff4d0-9175-40ac-a8bb-325c49733607"),
    geo_country = Some("US"),
    geo_region = Some("TX"),
    geo_city = Some("New York"),
    geo_zipcode = Some("94109"),
    geo_latitude = Some(37.443604),
    geo_longitude = Some(-122.4124),
    geo_region_name = Some("Florida"),
    ip_isp = Some("FDN Communications"),
    ip_organization = Some("Bouygues Telecom"),
    ip_domain = Some("nuvox.net"),
    ip_netspeed = Some("Cable/DSL"),
    page_url = Some("http://www.snowplowanalytics.com"),
    page_title = Some("On Analytics"),
    page_referrer = None,
    page_urlscheme = Some("http"),
    page_urlhost = Some("www.snowplowanalytics.com"),
    page_urlport = Some(80),
    page_urlpath = Some("/product/index.html"),
    page_urlquery = Some("id=GTM-DLRG"),
    page_urlfragment = Some("4-conclusion"),
    refr_urlscheme = None,
    refr_urlhost = None,
    refr_urlport = None,
    refr_urlpath = None,
    refr_urlquery = None,
    refr_urlfragment = None,
    refr_medium = None,
    refr_source = None,
    refr_term = None,
    mkt_medium = None,
    mkt_source = None,
    mkt_term = None,
    mkt_content = None,
    mkt_campaign = None,
    contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey(
            "org.schema",
            "WebPage",
            "jsonschema",
            SchemaVer.Full(1, 0, 0)
          ),
          JsonObject(
            ("genre", "blog".asJson),
            ("inLanguage", "en-US".asJson),
            ("datePublished", "2014-11-06T00:00:00Z".asJson),
            ("author", "Fred Blundun".asJson),
            ("breadcrumb", List("blog", "releases").asJson),
            ("keywords", List("snowplow", "javascript", "tracker", "event").asJson)
          ).asJson
        ),
        SelfDescribingData(
          SchemaKey(
            "org.w3",
            "PerformanceTiming",
            "jsonschema",
            SchemaVer.Full(1, 0, 0)
          ),
          JsonObject(
            ("navigationStart", 1415358089861L.asJson),
            ("unloadEventStart", 1415358090270L.asJson),
            ("unloadEventEnd", 1415358090287L.asJson),
            ("redirectStart", 0.asJson),
            ("redirectEnd", 0.asJson),
            ("fetchStart", 1415358089870L.asJson),
            ("domainLookupStart", 1415358090102L.asJson),
            ("domainLookupEnd", 1415358090102L.asJson),
            ("connectStart", 1415358090103L.asJson),
            ("connectEnd", 1415358090183L.asJson),
            ("requestStart", 1415358090183L.asJson),
            ("responseStart", 1415358090265L.asJson),
            ("responseEnd", 1415358090265L.asJson),
            ("domLoading", 1415358090270L.asJson),
            ("domInteractive", 1415358090886L.asJson),
            ("domContentLoadedEventStart", 1415358090968L.asJson),
            ("domContentLoadedEventEnd", 1415358091309L.asJson),
            ("domComplete", 0.asJson),
            ("loadEventStart", 0.asJson),
            ("loadEventEnd", 0.asJson)
          ).asJson
        )
      )
    ),
    se_category = None,
    se_action = None,
    se_label = None,
    se_property = None,
    se_value = None,
    unstruct_event = UnstructEvent(
      Some(
        SelfDescribingData(
          SchemaKey(
            "com.snowplowanalytics.snowplow",
            "link_click",
            "jsonschema",
            SchemaVer.Full(1, 0, 1)
          ),
          JsonObject(
            ("targetUrl", "http://www.example.com".asJson),
            ("elementClasses", List("foreground").asJson),
            ("elementId", "exampleLink".asJson)
          ).asJson
        )
      )
    ),
    tr_orderid = None,
    tr_affiliation = None,
    tr_total = None,
    tr_tax = None,
    tr_shipping = None,
    tr_city = None,
    tr_state = None,
    tr_country = None,
    ti_orderid = None,
    ti_sku = None,
    ti_name = None,
    ti_category = None,
    ti_price = None,
    ti_quantity = None,
    pp_xoffset_min = None,
    pp_xoffset_max = None,
    pp_yoffset_min = None,
    pp_yoffset_max = None,
    useragent = None,
    br_name = None,
    br_family = None,
    br_version = None,
    br_type = None,
    br_renderengine = None,
    br_lang = None,
    br_features_pdf = Some(true),
    br_features_flash = Some(false),
    br_features_java = None,
    br_features_director = None,
    br_features_quicktime = None,
    br_features_realplayer = None,
    br_features_windowsmedia = None,
    br_features_gears = None,
    br_features_silverlight = None,
    br_cookies = None,
    br_colordepth = None,
    br_viewwidth = None,
    br_viewheight = None,
    os_name = None,
    os_family = None,
    os_manufacturer = None,
    os_timezone = None,
    dvce_type = None,
    dvce_ismobile = None,
    dvce_screenwidth = None,
    dvce_screenheight = None,
    doc_charset = None,
    doc_width = None,
    doc_height = None,
    tr_currency = None,
    tr_total_base = None,
    tr_tax_base = None,
    tr_shipping_base = None,
    ti_currency = None,
    ti_price_base = None,
    base_currency = None,
    geo_timezone = None,
    mkt_clickid = None,
    mkt_network = None,
    etl_tags = None,
    dvce_sent_tstamp = None,
    refr_domain_userid = None,
    refr_dvce_tstamp = None,
    derived_contexts = Contexts(
      List(
        SelfDescribingData(
          SchemaKey(
            "com.snowplowanalytics.snowplow",
            "ua_parser_context",
            "jsonschema",
            SchemaVer.Full(1, 0, 0)
          ),
          JsonObject(
            ("useragentFamily", "IE".asJson),
            ("useragentMajor", "7".asJson),
            ("useragentMinor", "0".asJson),
            ("useragentPatch", Json.Null),
            ("useragentVersion", "IE 7.0".asJson),
            ("osFamily", "Windows XP".asJson),
            ("osMajor", Json.Null),
            ("osMinor", Json.Null),
            ("osPatch", Json.Null),
            ("osPatchMinor", Json.Null),
            ("osVersion", "Windows XP".asJson),
            ("deviceFamily", "Other".asJson)
          ).asJson
        )
      )
    ),
    domain_sessionid = Some("2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"),
    derived_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z")),
    event_vendor = Some("com.snowplowanalytics.snowplow"),
    event_name = Some("link_click"),
    event_format = Some("jsonschema"),
    event_version = Some("1-0-0"),
    event_fingerprint = Some("e3dbfa9cca0412c3d4052863cefb547f"),
    true_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z"))
  )

  val expectedJson = parse("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_created_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_referrer" : null,
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "refr_urlscheme" : null,
        "refr_urlhost" : null,
        "refr_urlport" : null,
        "refr_urlpath" : null,
        "refr_urlquery" : null,
        "refr_urlfragment" : null,
        "refr_medium" : null,
        "refr_source" : null,
        "refr_term" : null,
        "mkt_medium" : null,
        "mkt_source" : null,
        "mkt_term" : null,
        "mkt_content" : null,
        "mkt_campaign" : null,
        "contexts_org_schema_web_page_1" : [ {
          "genre" : "blog",
          "inLanguage" : "en-US",
          "datePublished" : "2014-11-06T00:00:00Z",
          "author" : "Fred Blundun",
          "breadcrumb" : [ "blog", "releases" ],
          "keywords" : [ "snowplow", "javascript", "tracker", "event" ]
        } ],
        "contexts_org_w3_performance_timing_1" : [ {
          "navigationStart" : 1415358089861,
          "unloadEventStart" : 1415358090270,
          "unloadEventEnd" : 1415358090287,
          "redirectStart" : 0,
          "redirectEnd" : 0,
          "fetchStart" : 1415358089870,
          "domainLookupStart" : 1415358090102,
          "domainLookupEnd" : 1415358090102,
          "connectStart" : 1415358090103,
          "connectEnd" : 1415358090183,
          "requestStart" : 1415358090183,
          "responseStart" : 1415358090265,
          "responseEnd" : 1415358090265,
          "domLoading" : 1415358090270,
          "domInteractive" : 1415358090886,
          "domContentLoadedEventStart" : 1415358090968,
          "domContentLoadedEventEnd" : 1415358091309,
          "domComplete" : 0,
          "loadEventStart" : 0,
          "loadEventEnd" : 0
        } ],
        "se_category" : null,
        "se_action" : null,
        "se_label" : null,
        "se_property" : null,
        "se_value" : null,
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
          "targetUrl" : "http://www.example.com",
          "elementClasses" : [ "foreground" ],
          "elementId" : "exampleLink"
        },
        "tr_orderid" : null,
        "tr_affiliation" : null,
        "tr_total" : null,
        "tr_tax" : null,
        "tr_shipping" : null,
        "tr_city" : null,
        "tr_state" : null,
        "tr_country" : null,
        "ti_orderid" : null,
        "ti_sku" : null,
        "ti_name" : null,
        "ti_category" : null,
        "ti_price" : null,
        "ti_quantity" : null,
        "pp_xoffset_min" : null,
        "pp_xoffset_max" : null,
        "pp_yoffset_min" : null,
        "pp_yoffset_max" : null,
        "useragent" : null,
        "br_name" : null,
        "br_family" : null,
        "br_version" : null,
        "br_type" : null,
        "br_renderengine" : null,
        "br_lang" : null,
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "br_features_java" : null,
        "br_features_director" : null,
        "br_features_quicktime" : null,
        "br_features_realplayer" : null,
        "br_features_windowsmedia" : null,
        "br_features_gears" : null,
        "br_features_silverlight" : null,
        "br_cookies" : null,
        "br_colordepth" : null,
        "br_viewwidth" : null,
        "br_viewheight" : null,
        "os_name" : null,
        "os_family" : null,
        "os_manufacturer" : null,
        "os_timezone" : null,
        "dvce_type" : null,
        "dvce_ismobile" : null,
        "dvce_screenwidth" : null,
        "dvce_screenheight" : null,
        "doc_charset" : null,
        "doc_width" : null,
        "doc_height" : null,
        "tr_currency" : null,
        "tr_total_base" : null,
        "tr_tax_base" : null,
        "tr_shipping_base" : null,
        "ti_currency" : null,
        "ti_price_base" : null,
        "base_currency" : null,
        "geo_timezone" : null,
        "mkt_clickid" : null,
        "mkt_network" : null,
        "etl_tags" : null,
        "dvce_sent_tstamp" : null,
        "refr_domain_userid" : null,
        "refr_dvce_tstamp" : null,
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }],
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z",
        "event_vendor": "com.snowplowanalytics.snowplow",
        "event_name": "link_click",
        "event_format": "jsonschema",
        "event_version": "1-0-0",
        "event_fingerprint": "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp": "2013-11-26T00:03:57.886Z"
      }""")
}

class TransformerSpec extends Specification {
  import TransformerSpec._

  "trauncateFields" should {
    "Correctly truncate event fields" in e1
    "Correctly transform event to shredded key/JSON pair" in e2
  }

  def e1 = {
    // truncateFields must not break event structure if all fields are in order
    Transformer.truncateFields(event.toJson(true), atomic) mustEqual event.toJson(true)

    // truncateFields must truncate specific long fields to length from atomic schema
    Transformer.truncateFields(event.copy(v_collector = "a" * 200).toJson(true), atomic) mustEqual event.copy(v_collector = "a" * 100).toJson(true)
    Transformer.truncateFields(event.copy(geo_region = Some("12345")).toJson(true), atomic) mustEqual event.copy(geo_region = Some("123")).toJson(true)
    Transformer.truncateFields(event.toJson(true), atomic + ("app_id" -> Some(5))) mustEqual event.copy(app_id = Some("angry")).toJson(true)
  }


  def e2 = {
    val transformed = Transformer.transform(event, atomicSchema)

    // transform must have valid shredded types
    transformed._1 mustEqual Set(
      "contexts_org_schema_web_page_1",
      "contexts_org_w3_performance_timing_1",
      "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1",
      "unstruct_event_com_snowplowanalytics_snowplow_link_click_1"
    )

    // transform must have atomic event with fields identical to expected
    // (but not necessarily the same field ordering)
    parse(transformed._2) mustEqual expectedJson
  }
}
