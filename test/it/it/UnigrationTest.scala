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

package it

import java.io.File
import java.nio.file.Files
import java.nio.file.StandardCopyOption._

import it.helper.{AppServerUnderTest, PSuites}
import it.suites._
import org.scalatest.{Args, SequentialNestedSuiteExecution, Status}
import org.scalatestplus.play.PlaySpec
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._
import uk.gov.hmrc.util.FileUtils

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

  override def runNestedSuites(args: Args): Status = {
    val s = new PSuites(
      new CollectionSuiteDB(appEndpoint, mongoTestConnection.uri)(app),
      new CollectionSuiteES(appEndpoint, esClient)(app),
      new WebdavSuite(appEndpoint, tmpDir)(app),
      new AdminSuite(appEndpoint)(app),
      new PingSuite(appEndpoint)(app)
    )
    s.runNestedSuites(args)
  }

  //-----------------------------------------------------------------------------------------------

  "ingest resource happy journey - to file" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await successful outcome,
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

  override def beforeAppServerStarts() {
    super.beforeAppServerStarts()
    FileUtils.deleteDir(tmpDir)
    val sample = getClass.getClassLoader.getResourceAsStream("exeter/1/sample/addressbase-premium-csv-sample-data.zip")
    val unpackFolder = new File(tmpDir, "download/exeter/1/sample")
    unpackFolder.mkdirs()
    Files.copy(sample, new File(unpackFolder, "SX9090.zip").toPath, REPLACE_EXISTING)
    sample.close()
  }

}

