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

import org.scalatest.{Args, Status, Suite, SuiteMixin}
import org.scalatestplus.play.ServerProvider
import play.api.libs.ws.WS
import play.api.test.Helpers._
import play.api.test.{FakeApplication, Helpers, TestServer}

trait AppServerUnderTest extends SuiteMixin with ServerProvider {
  this: Suite =>

  def embeddedMongoSettings: Map[String, String]

  def appConfiguration: Map[String, String]

  def beforeAppServerStarts() {}

  def afterAppServerStops() {}

  implicit override final lazy val app: FakeApplication = new FakeApplication(additionalConfiguration = embeddedMongoSettings ++ appConfiguration)

  /**
    * The port used by the `TestServer`.  By default this will be set to the result returned from
    * `Helpers.testServerPort`. You can override this to provide a different port number.
    */
  lazy val port: Int = Helpers.testServerPort

  lazy val appEndpoint = s"http://localhost:$port"

  abstract override def run(testName: Option[String], args: Args): Status = {
    beforeAppServerStarts()
    val testServer = TestServer(port, app)
    testServer.start()
    try {
      val newConfigMap = args.configMap + ("org.scalatestplus.play.app" -> app) + ("org.scalatestplus.play.port" -> port)
      val newArgs = args.copy(configMap = newConfigMap)
      val status = super.run(testName, newArgs)
      status.waitUntilCompleted()
      status
    }
    finally {
      testServer.stop()
      afterAppServerStops()
    }
  }

  def request(method: String, p: String) = {
    await(WS.url(appEndpoint + p).withMethod(method).withHeaders("User-Agent" -> "xyz").execute())
  }

  def get(p: String) = request("GET", p)


}
