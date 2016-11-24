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
import uk.gov.hmrc.util.FileUtils

class UnigrationTest extends PlaySpec with AppServerUnderTest with SequentialNestedSuiteExecution {

  //  private val tmp = System.getProperty("java.io.tmpdir")
  // this will get deleted so BE CAREFUL to include the subdirectory!
  private val tmpDir = new File("target", "ari")

  def appConfiguration: Map[String, String] = Map(
    "app.files.downloadFolder" -> s"$tmpDir/download",
    "app.files.outputFolder" -> s"$tmpDir/output"
  )

  override def runNestedSuites(args: Args): Status = {
    val s = new PSuites(
      new AdminSuite(appEndpoint)(app),
      new IngestFileSuite(appEndpoint, tmpDir)(app),
      new WebdavSuite(appEndpoint, tmpDir)(app),
      new CollectionSuiteES(appEndpoint, esClient)(app),
      new GoSuiteES(appEndpoint, esClient)(app),
      new PingSuite(appEndpoint)(app)
    )
    s.runNestedSuites(args)
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

  override def afterAppServerStops() {
    FileUtils.deleteDir(tmpDir)
    super.afterAppServerStops()
  }
}

