/*
 * Copyright 2016 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controllers

import java.io.File
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Paths}
import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec
import play.api.test.Helpers._

class IngestControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  def appConfiguration: Map[String, String] = Map(
    "app.files.rootFolder" -> "/var/tmp",
    "app.files.outputFolder" -> "/var/tmp"
  )

  override def beforeAppServerStarts() {
    setUpFixtures()
  }

  override def afterAppServerStops() {
    tearDownFixtures()
  }

  "ingest resource happy journey - to file" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await termination
       * observe quiet status
    """ in {
      waitWhile("/admin/status", "busy ingesting")

      verifyOK("/admin/status", "idle")

      val step2 = get("/ingest/to/file/abp/123456/test")
      step2.status mustBe OK

      verifyOK("/admin/status", "busy ingesting abp/123456/test")

      waitWhile("/admin/status", "busy ingesting abp/123456/test")

      verifyOK("/admin/status", "idle")

      val outFile = new File("/var/tmp/abp_123456.txt.gz")
      outFile.exists() mustBe true
    }
  }


  "ingest resource conflicted journey - to file" must {
    """
       * observe quiet status
       * start ingest
       * fail to start another ingest due to conflict
    """ in {
      verifyOK("/admin/status", "idle")

      val step2 = get("/ingest/to/file/abp/123456/test?bulkSize=1&loopDelay=30")
      step2.status mustBe OK

      val step3 = get("/ingest/to/file/abp/123456/test")
      step3.status mustBe CONFLICT
    }
  }


  "ingest resource happy journey - to Mongo" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await termination
       * observe quiet status
       * verify that the collection metadata contains completedAt with a sensible value
    """ in {
      val start = System.currentTimeMillis()

      waitWhile("/admin/status", "busy ingesting")

      verifyOK("/admin/status", "idle")

      val step2 = get("/ingest/to/db/abp/123456/test?bulkSize=5&loopDelay=0")
      step2.status mustBe OK

      verifyOK("/admin/status", "busy ingesting abp/123456/test")

      waitWhile("/admin/status", "busy ingesting abp/123456/test")

      verifyOK("/admin/status", "idle")

      val collection = casbahMongoConnection().getConfiguredDb("abp_123456_0")
      collection.size mustBe 30 // 29 records plus 1 metadata
      // (see similar tests in ExtractorTest)

      val metadata = collection.findOne(MongoDBObject("_id" -> "metadata")).get
      val completedAt = metadata.get("completedAt").asInstanceOf[Date].getTime
      assert(start <= completedAt)
      assert(completedAt <= System.currentTimeMillis())
    }
  }


  private def setUpFixtures() {
    val sample = getClass.getClassLoader.getResourceAsStream("SX9090-first3600.zip")
    val rootFolder = Paths.get("/var/tmp/abp/123456/test")
    rootFolder.toFile.mkdirs()
    Files.copy(sample, rootFolder.resolve("SX9090-first3600.zip"), REPLACE_EXISTING)
    sample.close()
  }

  private def tearDownFixtures() {
    val inFile = new File("/var/tmp/abp/123456/test/SX9090-first3600.zip")
    inFile.delete()
    val outFile = new File("/var/tmp/abp_123456.txt.gz")
    outFile.delete()
  }
}
