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
import java.nio.file.Files
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import helper.Utils._
import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec
import play.api.test.Helpers._

class IngestControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  private val tmp = System.getProperty("java.io.tmpdir")
  // this will get deleted so BE CAREFUL to include the subdirectory!
  private val tmpDir = new File(tmp, "ars")

  def appConfiguration: Map[String, String] = Map(
    "app.files.downloadFolder" -> s"$tmpDir/download",
    "app.files.unpackFolder" -> s"$tmpDir/unpack",
    "app.files.outputFolder" -> s"$tmpDir/output",
    "app.chronicleMap.blpu.mapSize" -> "50000",
    "app.chronicleMap.dpa.setSize" -> "5000",
    "app.chronicleMap.street.mapSize" -> "2000"
  )

  "ingest resource happy journey - to file" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await termination
       * observe quiet status
    """ in {
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val step2 = get("/ingest/to/file/exeter/1/sample")
      step2.status mustBe ACCEPTED

      verifyOK("/admin/status", "busy ingesting exeter/1/sample")

      assert(waitWhile("/admin/status", "busy ingesting exeter/1/sample", 100000) === true)

      verifyOK("/admin/status", "idle")

      val outFile = new File(s"$tmpDir/output/exeter_1.txt.gz")
      outFile.exists() mustBe true
      outFile.length() mustBe 689L
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

      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val step2 = get("/ingest/to/db/exeter/1/sample?bulkSize=5&loopDelay=0")
      step2.status mustBe ACCEPTED

      verifyOK("/admin/status", "busy ingesting exeter/1/sample")

      assert(waitWhile("/admin/status", "busy ingesting exeter/1/sample", 100000) === true)

      verifyOK("/admin/status", "idle")

      val collection = casbahMongoConnection().getConfiguredDb("exeter_1_0")
      collection.size mustBe 30 // 29 records plus 1 metadata
      // (see similar tests in ExtractorTest)

      val metadata = collection.findOne(MongoDBObject("_id" -> "metadata")).get
      val completedAt = metadata.get("completedAt").asInstanceOf[Date].getTime
      assert(start <= completedAt)
      assert(completedAt <= System.currentTimeMillis())
    }
  }

  override def beforeAppServerStarts() {
    deleteDir(tmpDir)
    val sample = getClass.getClassLoader.getResourceAsStream("exeter/1/sample/SX9090-first3600.zip")
    val unpackFolder = new File(tmpDir, "unpack/exeter/1/sample")
    unpackFolder.mkdirs()
    Files.copy(sample, new File(unpackFolder, "SX9090-first3600.zip").toPath, REPLACE_EXISTING)
    sample.close()
  }

  override def afterAppServerStops() {
    deleteDir(tmpDir)
  }
}
