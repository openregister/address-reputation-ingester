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

import it.helper.{EmbeddedElasticsearchSuite, EmbeddedWebdavStubSuite}
import it.suites._
import org.scalatest._
import org.scalatestplus.play.{OneServerPerSuite, PlaySpec}
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.{Application, Mode}
import uk.gov.hmrc.util.FileUtils

class UnigrationTest extends PlaySpec with OneServerPerSuite with EmbeddedElasticsearchSuite with EmbeddedWebdavStubSuite
  with SequentialNestedSuiteExecution with BeforeAndAfterAll {

  //  private val tmp = System.getProperty("java.io.tmpdir")
  // this will get deleted so BE CAREFUL to include the subdirectory!
  private val tmpDir = new File("target", "ari")

  implicit lazy val appEndpoint = s"http://localhost:$port"

  override implicit lazy val app: Application = new GuiceApplicationBuilder()
    .configure(Map(
      "app.files.downloadFolder" -> s"$tmpDir/download",
      "app.files.outputFolder" -> s"$tmpDir/output",
      "elastic.localMode" -> true,
      "app.remote.server" -> "http://localhost:8080/webdav"
    ))
    .in(Mode.Test)
    .build()

  override def nestedSuites: Vector[Suite] = {
    Vector(
      new AdminSuite,
      new IngestFileSuite(tmpDir),
      new WebdavSuite(tmpDir),
      new IndexSuite,
      new GoSuite,
      new PingSuite
    )
  }

  //-----------------------------------------------------------------------------------------------

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.deleteDir(tmpDir)
    val sample = getClass.getClassLoader.getResourceAsStream("exeter/1/sample/addressbase-premium-csv-sample-data.zip")
    val unpackFolder = new File(tmpDir, "download/exeter/1/sample")
    unpackFolder.mkdirs()
    Files.copy(sample, new File(unpackFolder, "SX9090.zip").toPath, REPLACE_EXISTING)
    sample.close()
  }
}

