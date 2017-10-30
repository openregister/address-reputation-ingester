/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package it.suites

import com.sksamuel.elastic4s.ElasticDsl._
import it.helper.ElasticsearchTestHelper._
import it.helper.{AppServerTestApi, Synopsis}
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{DoNotDiscover, FreeSpec, MustMatchers}
import play.api.Application
import play.api.libs.json.JsObject
import play.api.test.Helpers._
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es._
import uk.gov.hmrc.address.uk.Postcode
import uk.gov.hmrc.logging.StubLogger

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}

@DoNotDiscover
class IndexSuite()(implicit val app: Application, implicit val appEndpoint: String)
  extends FreeSpec with MustMatchers with AppServerTestApi {

  private implicit val ec = play.api.libs.concurrent.Execution.Implicits.defaultContext

  val idle = Synopsis.OkText("idle")

  //  val db_se1_9py = DbAddress("GB10091836674", List("Dorset House 27-45", "Stamford Street"), Some("London"), "SE1 9PY",
  //    Some("GB-ENG"), Some("UK"), Some(5840), Some("en"), Some(2), Some(1), None, None, Some("51.5069937,-0.1088798"))

  "es list indexes" - {
    """
       * return the sorted list of indexes
       * along with the completion dates (if present)
    """ in {

      val idx = IndexName("abp", Some(39), Some("ts5"))

      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 2, "abp" -> 2), statusLogger, ec)

      createSchema(idx, indexMetadata.clients)

      indexMetadata.writeCompletionDateTo(idx)

      waitForIndex(idx.formattedName)

      val request = newRequest("GET", "/indexes/es/list")
      val response = await(request.execute())

      assert(response.status === OK)
      assert((response.json \ "indexes").as[ListBuffer[JsObject]].length === 1)
      assert(((response.json \ "indexes") (0) \ "name").as[String] === idx.formattedName)
      assert(((response.json \ "indexes") (0) \ "size").as[Int] === 0)

      assert(waitUntil("/admin/status", idle) === idle)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es index endpoints" - {
    "drop unknown index should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/indexes/es/2001-12-31-01-02")
      val response = await(request.execute())
      response.status mustBe NOT_FOUND
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es ingest resource happy journey" - {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await successful outcome
       * observe quiet status
       * verify additional index metadata (loopDelay,bulkSize,includeDPA,includeLPI,prefer,streetFilter)
    """ in {
      val start = System.currentTimeMillis()

      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/es/exeter/1/sample?bulkSize=5&loopDelay=0&forceChange=true")
      val response = await(request.execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to es exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("exeter" -> 1, "abi" -> 1, "abp" -> 1), statusLogger, ec)
      waitForIndex("exeter", TimeValue.timeValueSeconds(3))

      val exeter1 = indexMetadata.existingIndexNamesLike(IndexName("exeter", Some(1))).head
      waitForIndex(exeter1.formattedName, TimeValue.timeValueSeconds(3))

      val metadata = indexMetadata.findMetadata(exeter1).get
      metadata.size mustBe Some(48737) // one less than DB because metadata stored in idx settings

      // (see similar tests in ExtractorTest)
      assert(metadata.bulkSize.get === "5")
      assert(metadata.loopDelay.get === "0")
      assert(metadata.includeDPA.get === "true")
      assert(metadata.includeLPI.get === "true")
      assert(metadata.streetFilter.get === "1")
      assert(metadata.prefer.get === "DPA")

      val ex46aw = await(findPostcode(exeter1, Postcode("EX4 6AW")))
      assert(ex46aw.size === 38)
      for (a <- ex46aw) {
        assert(a.postcode === "EX4 6AW")
        assert(a.town === Some("Exeter"))
      }
      assert(ex46aw.head.lines === List("33 Longbrook Street"))
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es ingest resource - errors" - {
    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/ingest/from/file/to/es/abp/not-a-number/full").status === BAD_REQUEST)
      //TODO fix this
      //assert(get("/ingest/from/file/to/es/abp/1/not-a-number").status === BAD_REQUEST)
    }
  }


  //-----------------------------------------------------------------------------------------------

  "switch-over resource happy journey" - {
    """
       * attempt to switch to existing index that has completedAt metadata
       * should change the nominated index
    """ in {
      val timestamp = IndexName.newTimestamp
      val idx = IndexName("abp", Some(40), Some(timestamp))

      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx.formattedName, IndexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      indexMetadata.writeCompletionDateTo(idx)

      val request = newRequest("GET", "/switch/es/abp/40/" + timestamp)
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp").get
      assert(indexName === idx)
    }
  }

  "switch-over resource error journeys" - {
    """
       * attempt to switch to non-existent index
       * should not change the nominated index
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialIndexName = indexMetadata.getIndexNameInUseFor("abp")

      val request = newRequest("GET", "/switch/es/abp/39/209902030405")
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp")
      assert(indexName === initialIndexName)
    }

    """
       * attempt to switch to existing index that has no completedAt metadata
       * should not change the nominated index
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialIndexName = indexMetadata.getIndexNameInUseFor("abp")

      val idx = IndexName("abp", Some(41), Some("209002030405"))
      createSchema(idx, indexMetadata.clients)
      waitForIndex(idx.formattedName)

      val request = newRequest("GET", "/switch/es/abp/41/209002030405")
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp")
      assert(indexName === initialIndexName)
    }

    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/switch/es/abp/not-a-number/1").status === BAD_REQUEST)
    }
  }

  private def await[T](future: Future[T], timeout: Duration = FiniteDuration(20, "s")): T = Await.result(future, timeout)
}
