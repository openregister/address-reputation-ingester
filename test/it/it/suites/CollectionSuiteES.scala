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

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import it.helper.{AppServerTestApi, ESHelper, Synopsis}
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{FreeSpec, MustMatchers}
import play.api.Application
import play.api.libs.json.JsObject
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._
import services.es.IndexMetadata
import services.model.StatusLogger
import services.mongo.CollectionName
import uk.gov.hmrc.address.services.es.ESSchema
import uk.gov.hmrc.address.uk.Postcode
import uk.gov.hmrc.logging.StubLogger

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}

class CollectionSuiteES(val appEndpoint: String, val esClient: ElasticClient)(implicit val app: Application)
  extends FreeSpec with MustMatchers with AppServerTestApi with ESHelper {

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  val idle = Synopsis.OkText("idle")

  //  val db_se1_9py = DbAddress("GB10091836674", List("Dorset House 27-45", "Stamford Street"), Some("London"), "SE1 9PY",
//    Some("GB-ENG"), Some("UK"), Some(5840), Some("en"), Some(2), Some(1), None, None, Some("51.5069937,-0.1088798"))

  "es list collections" - {
    """
       * return the sorted list of collections
       * along with the completion dates (if present)
    """ in {

      val idx = "abp_39_ts5"

      val statusLogger = new StatusLogger(new StubLogger(true))
      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 2, "abp" -> 2), statusLogger, ec)

      createSchema(idx, indexMetadata.clients)

      indexMetadata.writeCompletionDateTo(idx)

      waitForIndex(idx)

      val request = newRequest("GET", "/collections/es/list")
      val response = await(request.withAuth("admin", "password", BASIC).execute())

      assert(response.status === OK)
      assert((response.json \ "collections").as[ListBuffer[JsObject]].length === 1)
      assert(((response.json \ "collections") (0) \ "name").as[String] === idx)
      assert(((response.json \ "collections") (0) \ "size").as[Int] === 0)

      assert(waitUntil("/admin/status", idle) === idle)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es collection endpoints should be protected by basic auth" - {
    "list collections" in {
      val request = newRequest("GET", "/collections/es/list")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "drop collection" in {
      val request = newRequest("DELETE", "/collections/es/foo")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "clean" in {
      val request = newRequest("POST", "/collections/es/clean")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es collection endpoints" - {
    "drop unknown collection should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/collections/es/2001-12-31-01-02")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
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
       * verify that the collection metadata contains completedAt with a sensible value
       * verify additional collection metadata (loopDelay,bulkSize,includeDPA,includeLPI,prefer,streetFilter)
    """ in {
      val start = System.currentTimeMillis()

      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/es/exeter/1/sample?bulkSize=5&loopDelay=0&forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to es exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val statusLogger = new StatusLogger(new StubLogger(true))
      val indexMetadata = new IndexMetadata(List(esClient), false, Map("exeter" -> 1, "abi" -> 1, "abp" -> 1), statusLogger, ec)
      waitForIndex("exeter", TimeValue.timeValueSeconds(30))

      val exeter1 = indexMetadata.existingCollectionNamesLike(CollectionName("exeter", Some(1))).head
      waitForIndex(exeter1.toString, TimeValue.timeValueSeconds(30))

      val metadata = indexMetadata.findMetadata(exeter1).get
      metadata.size mustBe Some(48737) // one less than DB because metadata stored in idx settings

      // (see similar tests in ExtractorTest)
      val completedAt = metadata.completedAt.get.getTime
      assert(start <= completedAt)
      assert(completedAt <= System.currentTimeMillis())
      assert(metadata.bulkSize.get === "5")
      assert(metadata.loopDelay.get === "0")
      assert(metadata.includeDPA.get === "true")
      assert(metadata.includeLPI.get === "true")
      assert(metadata.streetFilter.get === "1")
      assert(metadata.prefer.get === "DPA")

      val ex46aw = await(findPostcode(exeter1.toString, Postcode("EX4 6AW")))
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

    """
       * when a wrong password is supplied
       * the response should be 401
    """ in {
      val request = newRequest("GET", "/ingest/from/file/to/es/exeter/1/sample")
      val response = await(request.withAuth("admin", "wrong", BASIC).execute())
      assert(response.status === UNAUTHORIZED)
    }
  }


  //-----------------------------------------------------------------------------------------------

  "switch-over resource happy journey" - {
    """
       * attempt to switch to existing collection that has completedAt metadata
       * should change the nominated collection
    """ in {
      val timestamp = CollectionName.newTimestamp
      val idx = CollectionName("abp", Some(40), Some(timestamp))

      val statusLogger = new StatusLogger(new StubLogger(true))
      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx.toString, IndexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      indexMetadata.writeCompletionDateTo(idx.toString)

      val request = newRequest("GET", "/switch/es/abp/40/" + timestamp)
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val collectionName = indexMetadata.getCollectionInUseFor("abp").get
      assert(collectionName === idx)
    }
  }

  "switch-over resource error journeys" - {
    """
       * attempt to switch to non-existent collection
       * should not change the nominated collection
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialCollectionName = indexMetadata.getCollectionInUseFor("abp")

      val request = newRequest("GET", "/switch/es/abp/39/209902030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val collectionName = indexMetadata.getCollectionInUseFor("abp")
      assert(collectionName === initialCollectionName)
    }

    """
       * attempt to switch to existing collection that has no completedAt metadata
       * should not change the nominated collection
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialCollectionName = indexMetadata.getCollectionInUseFor("abp")

      createSchema("abp_41_209002030405", indexMetadata.clients)
      waitForIndex("abp_41_209002030405")

      val request = newRequest("GET", "/switch/es/abp/41/209002030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val collectionName = indexMetadata.getCollectionInUseFor("abp")
      assert(collectionName === initialCollectionName)
    }

    """
       * when a wrong password is supplied
       * the response should be 401
    """ in {
      val request = newRequest("GET", "/switch/es/abp/42/200102030405")
      val response = await(request.withAuth("admin", "wrong", BASIC).execute())
      assert(response.status === UNAUTHORIZED)
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
