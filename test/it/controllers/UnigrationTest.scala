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

package controllers

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption._

import helper.AppServerUnderTest
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.SequentialNestedSuiteExecution
import org.scalatestplus.play.PlaySpec
import play.api.libs.json.JsObject
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._
import services.es.{ESSchema, IndexMetadata}
import services.mongo.{CollectionMetadata, CollectionName, MongoSystemMetadataStoreFactory}
import uk.gov.hmrc.address.admin.MetadataStore
import uk.gov.hmrc.logging.Stdout
import uk.gov.hmrc.util.FileUtils
import com.sksamuel.elastic4s.ElasticDsl._

import scala.collection.mutable.ListBuffer

//-------------------------------------------------------------------------------------------------
// This is a long test file to ensure that everything runs in sequence, not overlapping.
// It is also important to start/stop embedded mongo cleanly.
//
// Use the Folds, Luke!!!
//-------------------------------------------------------------------------------------------------

class UnigrationTest extends PlaySpec with AppServerUnderTest with SequentialNestedSuiteExecution {

  //  private val tmp = System.getProperty("java.io.tmpdir")
  // this will get deleted so BE CAREFUL to include the subdirectory!
  private val tmpDir = new File("target", "ars")

  def appConfiguration: Map[String, String] = Map(
    "app.files.downloadFolder" -> s"$tmpDir/download",
    "app.files.outputFolder" -> s"$tmpDir/output"
  )

  def waitForIndex(idx: String) {
    esClient.java.admin.cluster.prepareHealth(idx).setWaitForGreenStatus().setTimeout(TimeValue.timeValueSeconds(2)).get
  }

  //-----------------------------------------------------------------------------------------------

  "ping resource" must {
    "give a successful response" in {
      get("/ping").status mustBe OK
    }

    "give version information in the response body" in {
      (get("/ping").json \ "version").as[String] must not be empty
    }
  }

  //-----------------------------------------------------------------------------------------------

  // only light-weight admin tests are provided; mostly, manually testing happens here
  "admin endpoints should not cause error" must {
    "status" in {
      // already covered elsewhere
    }

    "fullStatus" in {
      val request = newRequest("GET", "/admin/fullStatus")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK)
      assert(response.body.nonEmpty)
    }

    "cancelCurrent" in {
      val request = newRequest("GET", "/admin/cancelCurrent")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK) // when not busy
    }

    "cancel" in {
      val request = newRequest("GET", "/admin/cancel/0")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK) // when not busy
    }

    "dirTree" in {
      val request = newRequest("GET", "/admin/dirTree")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK)
      assert(response.body.nonEmpty)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "admin endpoints should be protected by basic auth" must {
    "status" in {
      // speceial case - not protected
    }

    "fullStatus" in {
      val request = newRequest("GET", "/admin/fullStatus")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "cancelCurrent" in {
      val request = newRequest("GET", "/admin/cancelCurrent")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED) // when not busy
    }

    "cancel" in {
      val request = newRequest("GET", "/admin/cancel/0")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED) // when not busy
    }

    "dirTree" in {
      val request = newRequest("GET", "/admin/dirTree")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }
  }

  //-----------------------------------------------------------------------------------------------

  "list collections" must {
    """
       * return the sorted list of MongoDB collections
       * along with the completion dates (if present)
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      CollectionMetadata.writeCompletionDateTo(mongo.getConfiguredDb("abp_39_ts5"))

      val request = newRequest("GET", "/collections/db/list")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      assert(response.status === OK)
      //      assert(response.body === "foo")

      assert(waitUntil("/admin/status", "idle", 100000) === true)
      mongo.close()
    }

    """
       * return the sorted list of ES collections
       * along with the completion dates (if present)
    """ in {
      implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

      val idx = "abp_39_ts5"

      val indexMetadata = new IndexMetadata(List(esClient), false, Map("abi" -> 2, "abp" -> 2))

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx, indexMetadata.address, indexMetadata.metadata,
            ESSchema.Settings(1, 0, "1s"))
        } await
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
    "list Mongo collections" in {
      val request = newRequest("GET", "/collections/db/list")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "list ES collections" in {
      val request = newRequest("GET", "/collections/es/list")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "drop Mongo collection" in {
      val request = newRequest("DELETE", "/collections/db/foo")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "drop ES collection" in {
      val request = newRequest("DELETE", "/collections/es/foo")
      val response = await(request.execute())
      assert(response.status === UNAUTHORIZED)
    }

    "clean db" in {
      val request = newRequest("POST", "/collections/db/clean")
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
    "drop unknown Mongo collection should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/collections/db/2001-12-31-01-02")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe NOT_FOUND
    }

    "drop unknown ES collection should give NOT_FOUND" in {
      val request = newRequest("DELETE", "/collections/es/2001-12-31-01-02")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe NOT_FOUND
    }
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource happy journey - to file" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await termination,
       * observe quiet status
    """ in {
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val request = newRequest("GET", "/ingest/from/file/to/file/exeter/1/sample?forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe ACCEPTED

      verifyOK("/admin/status", "busy ingesting to file exeter/1/sample (forced)")

      assert(waitWhile("/admin/status", "busy ingesting to file exeter/1/sample (forced)", 100000) === true)

      verifyOK("/admin/status", "idle")

      val outputDir = new File(tmpDir, "output")
      val files = outputDir.listFiles()
      files.length mustBe 1
      val outFile = files.head
      outFile.exists() mustBe true
      outFile.length() mustBe 1109057L
    }
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource happy journey - to Mongo" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await termination
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

      val mongoConnection = casbahMongoConnection()
      val db = mongoConnection.getConfiguredDb
      val metadataStore = new MongoSystemMetadataStoreFactory().newStore(mongoConnection)
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
    }
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource - errors" must {
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

  "switch-over resource error journeys" must {
    """
       * attempt to switch to non-existent collection
       * should not change the nominated collection
    """ in {
      val mongo = casbahMongoConnection()
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
      val mongo = casbahMongoConnection()
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

  //-----------------------------------------------------------------------------------------------

  "switch-over resource happy journey" must {
    """
       * attempt to switch to existing collection that has completedAt metadata
       * should change the nominated collection
    """ in {
      val mongo = casbahMongoConnection()
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

  //-----------------------------------------------------------------------------------------------

  override def beforeAppServerStarts() {
    super.beforeAppServerStarts()
    FileUtils.deleteDir(tmpDir)
    val sample = getClass.getClassLoader.getResourceAsStream("exeter/1/sample/addressbase-premium-csv-sample-data.zip")
    val unpackFolder = new File(tmpDir, "download/exeter/1/sample")
    unpackFolder.mkdirs()
    Files.copy(sample, new File(unpackFolder, "SX9090.zip").toPath, REPLACE_EXISTING)
    sample.close()
  }

  //  override def afterAppServerStops() {
  //    super.afterAppServerStops()
  //    deleteDir(tmpDir)
  //  }

}

