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

package it.helper

import java.io.File

import com.sksamuel.elastic4s.ElasticClient
import org.scalatest._
import org.scalatestplus.play.ServerProvider
import play.api.Application
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.test.{Helpers, TestServer}
import uk.gov.hmrc.address.services.es.{ElasticDiskClientSettings, ElasticsearchHelper}
import uk.gov.hmrc.util.FileUtils

trait AppServerUnderTest extends SuiteMixin with ServerProvider with AppServerTestApi {
  this: Suite =>

  val esDataPath: String = System.getProperty("java.io.tmpdir") + "/es"

  lazy val esClient: ElasticClient = ElasticsearchHelper.buildDiskClient(ElasticDiskClientSettings(esDataPath, true))

  lazy val webdavStub = new WebdavStub(getClass.getClassLoader.getResource("ut").toURI.getPath)

  def appConfiguration: Map[String, String]

  def beforeAppServerStarts() {
    FileUtils.deleteDir(new File(esDataPath))
  }

  def afterAppServerStops() {}

  implicit override final lazy val app: Application = new GuiceApplicationBuilder().configure(
    appConfiguration ++
      Map(
        "elastic.localMode" -> true,
        "app.remote.server" -> "http://localhost:8080/webdav"
      )
  ).build()

  /**
    * The port used by the `TestServer`.  By default this will be set to the result returned from
    * `Helpers.testServerPort`. You can override this to provide a different port number.
    */
  lazy val port: Int = Helpers.testServerPort

  lazy val appEndpoint = s"http://localhost:$port"

  abstract override def run(testName: Option[String], args: Args): Status = {
    Thread.sleep(10)
    beforeAppServerStarts()
    val testServer = TestServer(port, app)
    testServer.start()
    webdavStub.start()
    try {
      val newConfigMap = args.configMap + ("org.scalatestplus.play.app" -> app) + ("org.scalatestplus.play.port" -> port)
      val newArgs = args.copy(configMap = newConfigMap)
      val status = super.run(testName, newArgs)
      status.waitUntilCompleted()
      status
    }
    finally {
      webdavStub.stop()
      testServer.stop()
      afterAppServerStops()
      esClient.close()
    }
  }
}

