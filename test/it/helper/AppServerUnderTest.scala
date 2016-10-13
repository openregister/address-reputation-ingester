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

package helper

import java.io.File

import com.github.simplyscala.MongoEmbedDatabase
import com.sksamuel.elastic4s.ElasticClient
import org.elasticsearch.common.settings.Settings
import org.scalatest._
import org.scalatestplus.play.ServerProvider
import play.api.test.{FakeApplication, Helpers, TestServer}
import uk.gov.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.util.FileUtils

trait AppServerUnderTest extends SuiteMixin with ServerProvider with MongoEmbedDatabase with AppServerTestApi {
  this: Suite =>

  val esDataPath = System.getProperty("java.io.tmpdir") + "/es"

  val mongoTestConnection = new MongoTestConnection(mongoStart())

  val esSettings = Settings.settingsBuilder()
    .put("http.enabled", false)
    .put("path.home", esDataPath)

  lazy val esClient = ElasticClient.local(esSettings.build)

  lazy val webdavStub = new helper.WebdavStub(getClass.getClassLoader.getResource("ut").toURI.getPath)

  def appConfiguration: Map[String, String]

  def casbahMongoConnection() = new CasbahMongoConnection(mongoTestConnection.uri)

  def beforeAppServerStarts() {
    FileUtils.deleteDir(new File(esDataPath))
  }

  def afterAppServerStops() {}

  implicit override final lazy val app: FakeApplication = new FakeApplication(
    additionalConfiguration = appConfiguration ++
      Map(
        mongoTestConnection.configItem, "elastic.localmode" -> true,
        "app.remote.server" -> "http://localhost:8080/webdav")
      )

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
      mongoTestConnection.stop()
      esClient.close()
    }
  }
}

