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

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID

import io.circe.literal._
import io.circe.parser._

import cats.implicits._

import scala.collection.JavaConverters._

import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.model.{ResourceNotFoundException, ScanRequest}

import com.snowplowanalytics.snowplow.eventsmanifest.{DynamoDbManifest, EventsManifest, EventsManifestConfig}
import com.snowplowanalytics.snowflake.transformer.TransformerJobSpec

object CrossBatchDeduplicationSpec {
  import TransformerJobSpec._

  // original duplicated event_id
  val dupeUuid = UUID.fromString("1799a90f-f570-4414-b91a-b0db8f39cc2e")
  val dupeFp = "bed9a39a0917874d2ff072033a6413d8"

  val uniqueUuid = UUID.fromString("e271698a-3e86-4b2f-bb1b-f9f7aa5666c1")
  val uniqueFp = "e79bef64f3185e9d7c10d5dfdf27b9a3"

  val inbatchDupeUuid = UUID.fromString("2718ac0f-f510-4314-a98a-cfdb8f39abe4")
  val inbatchDupeFp = "aba1c39a091787aa231072033a647caa"

  // ETL Timestamps (use current timestamp as we cannot use timestamps from past)
  val previousEtlTstamp = Instant.now().minusSeconds(3600 * 2)
  val currentEtlTstamp = Instant.now()

  private val Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
  implicit class InstantOps(time: Instant) {
    def formatted: String = {
      time.atOffset(ZoneOffset.UTC).format(Formatter)
    }
  }

  // Events, including one cross-batch duplicate and in-batch duplicates
  val lines = Lines(
    // In-batch unique event that has natural duplicate in dupe storage
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 07:16:07.000	2016-11-27 07:16:07.333	page_view	$dupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$dupeFp	""",

    // In-batch natural duplicates
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 06:26:17.000	2016-11-27 06:26:17.333	page_view	$inbatchDupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$inbatchDupeFp	""",
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 06:26:17.000	2016-11-27 06:26:17.333	page_view	$inbatchDupeUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		185.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$inbatchDupeFp	""",

    // Fully unique event
    s"""blog	web	${currentEtlTstamp.formatted}	2016-11-27 18:12:17.000	2016-11-27 17:00:01.333	page_view	$uniqueUuid		blogTracker	js-2.7.0-rc2	clj-1.1.0-tom-0.2.0	hadoop-1.8.0-common-0.24.0		199.124.153.x	531497290	1f9b3980-6619-4d75-a6c9-8253c76c3bfb	18	5beb1f92-d4fb-4020-905c-f659929c8ab5												http://chuwy.me/scala-blocks.html	Scala Code Blocks	http://chuwy.me/	http	chuwy.me	80	/scala-blocks.html			http	chuwy.me	80	/			internal																																	Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36	Chrome	Chrome	54.0.2840.98	Browser	WEBKIT		1	1	0	0	0	0	0	0	0	1	24	1280	726	Mac OS X	Mac OS X	Apple Inc.	Asia/Omsk	Computer	0	1280	800	UTF-8	1280	4315												2016-11-27 07:16:07.340			{"schema":"iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-1","data":[{"schema":"iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0","data":{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}}]}	395e4506-37a3-4074-8de2-d8c75fb17d4a	2016-11-27 07:16:06.993	com.snowplowanalytics.snowplow	page_view	jsonschema	1-0-0	$uniqueFp	"""
  )

  object expected {
    val transformedEvents = List(
      json"""{"page_urlhost":"chuwy.me","br_features_realplayer":false,"etl_tstamp":${currentEtlTstamp.toString},"dvce_ismobile":false,"geo_latitude":null,"refr_medium":"internal","ti_orderid":null,"br_version":"54.0.2840.98","base_currency":null,"v_collector":"clj-1.1.0-tom-0.2.0","mkt_content":null,"collector_tstamp":"2016-11-27T18:12:17Z","os_family":"Mac OS X","ti_sku":null,"event_vendor":"com.snowplowanalytics.snowplow","network_userid":"5beb1f92-d4fb-4020-905c-f659929c8ab5","br_renderengine":"WEBKIT","br_lang":null,"tr_affiliation":null,"ti_quantity":null,"ti_currency":null,"geo_country":null,"user_fingerprint":"531497290","mkt_medium":null,"page_urlscheme":"http","ti_category":null,"pp_yoffset_min":null,"br_features_quicktime":false,"event":"page_view","refr_urlhost":"chuwy.me","user_ipaddress":"199.124.153.x","br_features_pdf":true,"page_referrer":"http://chuwy.me/","doc_height":4315,"refr_urlscheme":"http","geo_region":null,"geo_timezone":null,"page_urlfragment":null,"br_features_flash":true,"os_manufacturer":"Apple Inc.","mkt_clickid":null,"ti_price":null,"br_colordepth":"24","event_format":"jsonschema","tr_total":null,"pp_xoffset_min":null,"doc_width":1280,"geo_zipcode":null,"br_family":"Chrome","tr_currency":null,"useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36","event_name":"page_view","os_name":"Mac OS X","page_urlpath":"/scala-blocks.html","br_name":"Chrome","ip_netspeed":null,"page_title":"Scala Code Blocks","ip_organization":null,"dvce_created_tstamp":"2016-11-27T17:00:01.333Z","br_features_gears":false,"dvce_type":"Computer","dvce_sent_tstamp":"2016-11-27T07:16:07.340Z","se_action":null,"br_features_director":false,"se_category":null,"ti_name":null,"user_id":null,"refr_urlquery":null,"true_tstamp":null,"geo_longitude":null,"mkt_term":null,"v_tracker":"js-2.7.0-rc2","os_timezone":"Asia/Omsk","br_type":"Browser","br_features_windowsmedia":false,"event_version":"1-0-0","dvce_screenwidth":1280,"refr_dvce_tstamp":null,"se_label":null,"domain_sessionid":"395e4506-37a3-4074-8de2-d8c75fb17d4a","domain_userid":"1f9b3980-6619-4d75-a6c9-8253c76c3bfb","page_urlquery":null,"refr_term":null,"name_tracker":"blogTracker","tr_tax_base":null,"dvce_screenheight":800,"mkt_campaign":null,"refr_urlfragment":null,"contexts_com_snowplowanalytics_snowplow_ua_parser_context_1":[{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}],"tr_shipping":null,"tr_shipping_base":null,"br_features_java":false,"br_viewwidth":1280,"geo_city":null,"br_viewheight":726,"refr_domain_userid":null,"br_features_silverlight":false,"ti_price_base":null,"tr_tax":null,"br_cookies":true,"tr_total_base":null,"refr_urlport":80,"derived_tstamp":"2016-11-27T07:16:06.993Z","app_id":"blog","ip_isp":null,"geo_region_name":null,"pp_yoffset_max":null,"ip_domain":null,"domain_sessionidx":18,"pp_xoffset_max":null,"mkt_source":null,"page_urlport":80,"se_property":null,"platform":"web","event_id":$uniqueUuid,"refr_urlpath":"/","mkt_network":null,"se_value":null,"page_url":"http://chuwy.me/scala-blocks.html","etl_tags":null,"tr_orderid":null,"tr_state":null,"txn_id":null,"refr_source":null,"tr_country":null,"tr_city":null,"doc_charset":"UTF-8","event_fingerprint":"e79bef64f3185e9d7c10d5dfdf27b9a3","v_etl":"hadoop-1.8.0-common-0.24.0"}""",
      json"""{"page_urlhost":"chuwy.me","br_features_realplayer":false,"etl_tstamp":${currentEtlTstamp.toString},"dvce_ismobile":false,"geo_latitude":null,"refr_medium":"internal","ti_orderid":null,"br_version":"54.0.2840.98","base_currency":null,"v_collector":"clj-1.1.0-tom-0.2.0","mkt_content":null,"collector_tstamp":"2016-11-27T06:26:17Z","os_family":"Mac OS X","ti_sku":null,"event_vendor":"com.snowplowanalytics.snowplow","network_userid":"5beb1f92-d4fb-4020-905c-f659929c8ab5","br_renderengine":"WEBKIT","br_lang":null,"tr_affiliation":null,"ti_quantity":null,"ti_currency":null,"geo_country":null,"user_fingerprint":"531497290","mkt_medium":null,"page_urlscheme":"http","ti_category":null,"pp_yoffset_min":null,"br_features_quicktime":false,"event":"page_view","refr_urlhost":"chuwy.me","user_ipaddress":"185.124.153.x","br_features_pdf":true,"page_referrer":"http://chuwy.me/","doc_height":4315,"refr_urlscheme":"http","geo_region":null,"geo_timezone":null,"page_urlfragment":null,"br_features_flash":true,"os_manufacturer":"Apple Inc.","mkt_clickid":null,"ti_price":null,"br_colordepth":"24","event_format":"jsonschema","tr_total":null,"pp_xoffset_min":null,"doc_width":1280,"geo_zipcode":null,"br_family":"Chrome","tr_currency":null,"useragent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36","event_name":"page_view","os_name":"Mac OS X","page_urlpath":"/scala-blocks.html","br_name":"Chrome","ip_netspeed":null,"page_title":"Scala Code Blocks","ip_organization":null,"dvce_created_tstamp":"2016-11-27T06:26:17.333Z","br_features_gears":false,"dvce_type":"Computer","dvce_sent_tstamp":"2016-11-27T07:16:07.340Z","se_action":null,"br_features_director":false,"se_category":null,"ti_name":null,"user_id":null,"refr_urlquery":null,"true_tstamp":null,"geo_longitude":null,"mkt_term":null,"v_tracker":"js-2.7.0-rc2","os_timezone":"Asia/Omsk","br_type":"Browser","br_features_windowsmedia":false,"event_version":"1-0-0","dvce_screenwidth":1280,"refr_dvce_tstamp":null,"se_label":null,"domain_sessionid":"395e4506-37a3-4074-8de2-d8c75fb17d4a","domain_userid":"1f9b3980-6619-4d75-a6c9-8253c76c3bfb","page_urlquery":null,"refr_term":null,"name_tracker":"blogTracker","tr_tax_base":null,"dvce_screenheight":800,"mkt_campaign":null,"refr_urlfragment":null,"contexts_com_snowplowanalytics_snowplow_ua_parser_context_1":[{"useragentFamily":"Chrome","useragentMajor":"54","useragentMinor":"0","useragentPatch":"2840","useragentVersion":"Chrome 54.0.2840","osFamily":"MacOS X","osMajor":"10","osMinor":"11","osPatch":"6","osPatchMinor":null,"osVersion":"Mac OS X 10.11.6","deviceFamily":"Other"}],"tr_shipping":null,"tr_shipping_base":null,"br_features_java":false,"br_viewwidth":1280,"geo_city":null,"br_viewheight":726,"refr_domain_userid":null,"br_features_silverlight":false,"ti_price_base":null,"tr_tax":null,"br_cookies":true,"tr_total_base":null,"refr_urlport":80,"derived_tstamp":"2016-11-27T07:16:06.993Z","app_id":"blog","ip_isp":null,"geo_region_name":null,"pp_yoffset_max":null,"ip_domain":null,"domain_sessionidx":18,"pp_xoffset_max":null,"mkt_source":null,"page_urlport":80,"se_property":null,"platform":"web","event_id":$inbatchDupeUuid,"refr_urlpath":"/","mkt_network":null,"se_value":null,"page_url":"http://chuwy.me/scala-blocks.html","etl_tags":null,"tr_orderid":null,"tr_state":null,"txn_id":null,"refr_source":null,"tr_country":null,"tr_city":null,"doc_charset":"UTF-8","event_fingerprint":"aba1c39a091787aa231072033a647caa","v_etl":"hadoop-1.8.0-common-0.24.0"}"""
    )
  }

  /** Logic required to connect to duplicate storage and mock data */
  object Storage {
    import TransformerJobSpec._

    private val client = AmazonDynamoDBClientBuilder
      .standard()
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:8000", dynamodbDuplicateStorageRegion))
      .build()

    /** Helper container class to hold components stored in DuplicationStorage */
    case class DuplicateTriple(eventId: UUID, eventFingerprint: String, etlTstamp: Instant)

    val randomUUID = UUID.randomUUID()
    // Events processed in previous runs
    val dupeStorage = List(
      // Event stored during last ETL, which duplicate will be present
      DuplicateTriple(dupeUuid, dupeFp, previousEtlTstamp),
      // Same payload, but unique id
      DuplicateTriple(randomUUID, dupeFp, previousEtlTstamp),
      // Synthetic duplicate
      DuplicateTriple(dupeUuid, "randomFp", previousEtlTstamp),
      // Event written during last (failed) ETL
      DuplicateTriple(uniqueUuid, uniqueFp, currentEtlTstamp)
    )

    /** Delete and re-create local DynamoDB table designed to store duplicate triples */
    private[good] def prepareLocalTable() = {
      val storage = getStorage().valueOr(e => throw new RuntimeException(e))
      dupeStorage.foreach { case DuplicateTriple(eid, fp, etlTstamp) =>
        storage.put(eid, fp, etlTstamp)
      }
    }

    /**
      * Initialize duplicate storage from environment variables.
      * It'll delete table if it exist and recreate new one
      */
    private def getStorage() = {
      def build(): EventsManifestConfig.DynamoDb = {
        try {   // Send request to delete previously created table and wait unit it is deleted
          println("Deleting the table")
          client.deleteTable(dynamodbDuplicateStorageTable)
          new Table(client, dynamodbDuplicateStorageTable).waitForDelete()
          println("Table has been deleted")
        } catch {
          case _: ResourceNotFoundException =>
            println("The table didn't exist, skipping")
        }

        val config = EventsManifestConfig.DynamoDb(
          id = None,
          name = "Duplicate Storage Integration Test",
          auth = Some(EventsManifestConfig.DynamoDb.Credentials("fake", "fake")),
          awsRegion = dynamodbDuplicateStorageRegion,
          dynamodbTable = dynamodbDuplicateStorageTable
        )

        config
      }

      val config = build()

      EventsManifest.initStorage(config) match {
        case Right(t) => t.asRight[Throwable]
        case Left(_) =>
          DynamoDbManifest.createTable(client, config.dynamodbTable, None, None)
          println("Table has been created")
          val table = DynamoDbManifest.checkTable(client, dynamodbDuplicateStorageTable)
          new DynamoDbManifest(client, table).asRight
      }
    }

    /** Get list of item ids that were stored during the test */
    private[good] def getStoredItems: List[String] = {
      val res = new ScanRequest().withTableName(dynamodbDuplicateStorageTable)
      val outcome = client.scan(res)
      outcome.getItems.asScala.map(_.asScala).flatMap(_.get("eventId")).toList.map(_.getS)
    }
  }
}

class CrossBatchDeduplicationSpec extends TransformerJobSpec {
  import TransformerJobSpec._
  override def appName = "cross-batch-deduplication"
  sequential
  "A job which is provided with a two events with same event_id" should {
    CrossBatchDeduplicationSpec.Storage.prepareLocalTable()
    runTransformerJob(CrossBatchDeduplicationSpec.lines, inBatchDedup = true, crossBatchDedup = true)

    val expectedFiles = scala.collection.mutable.ArrayBuffer.empty[String]

    "remove cross-batch duplicate and store left event in good output" in {
      val Some((lines, f)) = readPartFile(dirs.output, "")
      expectedFiles += f
      lines.map(parse).sequence must beRight(CrossBatchDeduplicationSpec.expected.transformedEvents)
    }

    "shred two unique events out of cross-batch and in-batch duplicates" in {
      val Some((lines, f)) = readPartFile(dirs.output, "")
      expectedFiles += f
      val eventIds = lines.map(l => parse(l).flatMap(j => j.hcursor.downField("event_id").as[String])).sequence
      val expectedUuids = Seq(
        CrossBatchDeduplicationSpec.uniqueUuid,
        CrossBatchDeduplicationSpec.inbatchDupeUuid
      ).map(_.toString)
      eventIds must beRight(expectedUuids)
    }

    "store exactly 5 known rows in DynamoDB" in {
      val expectedEids = Seq(
        CrossBatchDeduplicationSpec.dupeUuid,
        CrossBatchDeduplicationSpec.dupeUuid,
        CrossBatchDeduplicationSpec.inbatchDupeUuid,
        CrossBatchDeduplicationSpec.uniqueUuid,
        CrossBatchDeduplicationSpec.Storage.randomUUID
      ).map(_.toString)
      CrossBatchDeduplicationSpec.Storage.getStoredItems.sorted mustEqual expectedEids.sorted
    }

    "not shred any unexpected JSONs" in {
      listFilesWithExclusions(dirs.output, expectedFiles.toList) must beEmpty
    }

    "not write any bad row JSONs" in {
      dirs.badRows must beEmptyDir
    }
  }
}
