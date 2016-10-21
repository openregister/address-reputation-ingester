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
import it.helper.{AppServerTestApi, ESHelper}
import org.scalatest.{MustMatchers, WordSpec}
import play.api.Application
import play.api.libs.json.JsObject
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._
import services.es.IndexMetadata
import uk.gov.hmrc.address.services.es.ESSchema
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.unit.TimeValue
import services.mongo.CollectionName

import scala.collection.mutable.ListBuffer

class CollectionSuiteES(val appEndpoint: String, val esClient: ElasticClient)(implicit val app: Application)
  extends WordSpec with MustMatchers with AppServerTestApi with ESHelper {

  "list collections" must {
    """
       * return the sorted list of ES collections
       * along with the completion dates (if present)
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val idx = "abp_39_ts5"

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 2, "abp" -> 2))

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx, indexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      indexMetadata.writeCompletionDateTo(idx)

      waitForIndex(idx)

      val request = newRequest("GET", "/collections/es/list")
      val response = await(request.withAuth("admin", "password", BASIC).execute())

      assert(response.status === OK)
      assert((response.json \ "collections").as[ListBuffer[JsObject]].length === 1)
      assert(((response.json \ "collections") (0) \ "name").as[String] === idx)
      assert(((response.json \ "collections") (0) \ "size").as[Int] === 0)

      assert(waitUntil("/admin/status", "idle", 100000) === true)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "collection endpoints should be protected by basic auth" must {
    "list ES collections" in {
      val request = newRequest("GET", "/collections/es/list")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "drop ES collection" in {
      val request = newRequest("DELETE", "/collections/es/foo")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "clean es" in {
      val request = newRequest("POST", "/collections/es/clean")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "collection endpoints" must {
    "drop unknown ES collection should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/collections/es/2001-12-31-01-02")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe NOT_FOUND
    }
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource happy journey - to es" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await successful outcome
       * observe quiet status
       * verify that the collection metadata contains completedAt with a sensible value
       * verify additional collection metadata (loopDelay,bulkSize,includeDPA,includeLPI,prefer,streetFilter)
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val start = System.currentTimeMillis()

      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val request = newRequest("GET", "/ingest/from/file/to/es/exeter/1/sample?bulkSize=5&loopDelay=0&forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe ACCEPTED

      verifyOK("/admin/status", "busy ingesting to es exeter/1/sample (forced)")

      waitWhile("/admin/status", "busy ingesting to es exeter/1/sample (forced)", 100000)

      verifyOK("/admin/status", "idle")

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1))
      waitForIndex("exeter", TimeValue.timeValueSeconds(30))
      val exeter1 = indexMetadata.existingCollectionNamesLike(CollectionName("exeter", Some(1))).head
      val metadata = indexMetadata.findMetadata(exeter1).get
      metadata.size mustBe 48737 // one less than DB because metadata stored in idx settings

      // (see similar tests in ExtractorTest)
      val completedAt = metadata.completedAt.get.getTime
      assert(start <= completedAt)
      assert(completedAt <= System.currentTimeMillis())
      assert(metadata.bulkSize.get === "5")
      assert(metadata.loopDelay.get === "0")
      assert(metadata.includeDPA.get === "true")
      assert(metadata.includeLPI.get === "true")
      assert(metadata.streetFilter.get === "1")
    }
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource - es - errors" must {
    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/ingest/from/file/to/es/abp/not-a-number/full").status === BAD_REQUEST)
      //TODO fix this assert(get("/ingest/to/db/abp/1/not-a-number").status === BAD_REQUEST)
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

  "switch-over resource happy journey - es" must {
    """
       * attempt to switch to existing collection that has completedAt metadata
       * should change the nominated collection
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val idx = "abp_39_200102030405"

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1))

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx, indexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      val request = newRequest("GET", "/switch/es/abp/39/200102030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = indexMetadata.getCollectionInUseFor("abp").get.toString
      assert(collectionName === "abp_39_200102030405")
    }
  }

  "switch-over resource error journeys - es" must {
    """
       * attempt to switch to non-existent collection
       * should not change the nominated collection
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1))
      val initialCollectionName = indexMetadata.getCollectionInUseFor("abp")

      val request = newRequest("GET", "/switch/es/abp/39/209902030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = indexMetadata.getCollectionInUseFor("abp")
      assert(collectionName === initialCollectionName)
    }

    """
       * attempt to switch to existing collection that has no completedAt metadata
       * should not change the nominated collection
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 1, "abp" -> 1))
      val initialCollectionName = indexMetadata.getCollectionInUseFor("abp")

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition("209902030405", indexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      val request = newRequest("GET", "/switch/es/abp/39/209002030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = indexMetadata.getCollectionInUseFor("abp")
      assert(collectionName === initialCollectionName)
    }

    """
       * when a wrong password is supplied
       * the response should be 401
    """ in {
      val request = newRequest("GET", "/switch/es/abp/39/200102030405")
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
}
