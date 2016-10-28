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

import it.helper.AppServerTestApi
import org.scalatest.{FreeSpec, MustMatchers}
import play.api.Application
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._
import services.mongo.{CollectionMetadata, CollectionName, MongoSystemMetadataStoreFactory}
import uk.gov.hmrc.address.admin.MetadataStore
import uk.gov.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.logging.Stdout

class CollectionSuiteDB(val appEndpoint: String, mongoUri: String)(implicit val app: Application)
  extends FreeSpec with MustMatchers with AppServerTestApi {

  "db list collections" - {
    """
       * return the sorted list of collections
       * along with the completion dates (if present)
    """ in {
      val mongo = new CasbahMongoConnection(mongoUri)
      CollectionMetadata.writeCompletionDateTo(mongo.getConfiguredDb("abp_39_ts5"))

      val request = newRequest("GET", "/collections/db/list")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK)

      assert(waitUntil("/admin/status", "idle", 100000) === true)
      mongo.close()
    }
  }

  //-----------------------------------------------------------------------------------------------

  "db collection endpoints should be protected by basic auth" - {
    "list collections" in {
      val request = newRequest("GET", "/collections/db/list")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "drop collection" in {
      val request = newRequest("DELETE", "/collections/db/foo")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "clean" in {
      val request = newRequest("POST", "/collections/db/clean")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "db collection endpoints" - {
    "drop unknown collection should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/collections/db/2001-12-31-01-02")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe NOT_FOUND
    }
  }

  //-----------------------------------------------------------------------------------------------

  "db ingest resource happy journey" - {
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

      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val request = newRequest("GET", "/ingest/from/file/to/db/exeter/1/sample?bulkSize=5&loopDelay=0&forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe ACCEPTED

      verifyOK("/admin/status", "busy ingesting to db exeter/1/sample (forced)")

      waitWhile("/admin/status", "busy ingesting to db exeter/1/sample (forced)", 100000)

      verifyOK("/admin/status", "idle")

      val mongo = new CasbahMongoConnection(mongoUri)
      val db = mongo.getConfiguredDb
      val metadataStore = new MongoSystemMetadataStoreFactory().newStore(mongo)
      val collectionMetadata = new CollectionMetadata(db, metadataStore)
      val exeter1 = collectionMetadata.existingCollectionNamesLike(CollectionName("exeter", Some(1))).head
      val collection = db(exeter1.toString)
      collection.size mustBe 48738 // 48737 records plus 1 metadata
      // (see similar tests in ExtractorTest)

      val metadata = collectionMetadata.findMetadata(exeter1).get
      val completedAt = metadata.completedAt.get.getTime
      assert(start <= completedAt)
      assert(completedAt <= System.currentTimeMillis())
      assert(metadata.bulkSize.get === "5")
      assert(metadata.loopDelay.get === "0")
      assert(metadata.includeDPA.get === "true")
      assert(metadata.includeLPI.get === "true")
      assert(metadata.streetFilter.get === "1")
      mongo.close()
    }
  }

  //-----------------------------------------------------------------------------------------------

  "db ingest resource - errors" - {
    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/ingest/from/file/to/db/abp/not-a-number/full").status === BAD_REQUEST)
      //TODO fix this assert(get("/ingest/to/db/abp/1/not-a-number").status === BAD_REQUEST)
    }

    """
       * when a wrong password is supplied
       * the response should be 401
    """ in {
      val request = newRequest("GET", "/ingest/from/file/to/db/exeter/1/sample")
      val response = await(request.withAuth("admin", "wrong", BASIC).execute())
      assert(response.status === UNAUTHORIZED)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "switch-over resource happy journey - db" - {
    """
       * attempt to switch to existing collection that has completedAt metadata
       * should change the nominated collection
    """ in {
      val mongo = new CasbahMongoConnection(mongoUri)
      val admin = new MetadataStore(mongo, Stdout)
      CollectionMetadata.writeCreationDateTo(mongo.getConfiguredDb("abp_39_200102030405"))
      CollectionMetadata.writeCompletionDateTo(mongo.getConfiguredDb("abp_39_200102030405"))

      val request = newRequest("GET", "/switch/db/abp/39/200102030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === "abp_39_200102030405")

      mongo.close()
    }
  }

  "db switch-over resource error journeys" - {
    """
       * attempt to switch to non-existent collection
       * should not change the nominated collection
    """ in {
      val mongo = new CasbahMongoConnection(mongoUri)
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get

      val request = newRequest("GET", "/switch/db/abp/39/200102030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }

    """
       * attempt to switch to existing collection that has no completedAt metadata
       * should not change the nominated collection
    """ in {
      val mongo = new CasbahMongoConnection(mongoUri)
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get
      CollectionMetadata.writeCreationDateTo(mongo.getConfiguredDb("abp_39_200102030405"))

      val request = newRequest("GET", "/switch/db/abp/39/200102030405")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }

    """
       * when a wrong password is supplied
       * the response should be 401
    """ in {
      val request = newRequest("GET", "/switch/db/abp/39/200102030405")
      val response = await(request.withAuth("admin", "wrong", BASIC).execute())
      assert(response.status === UNAUTHORIZED)
    }

    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/switch/db/abp/not-a-number/1").status === BAD_REQUEST)
    }
  }
}
