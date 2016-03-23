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

import helper.{EmbeddedMongoSuite, AppServerUnderTest}
import org.scalatestplus.play._
import play.api.test.Helpers._

class PingTest extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  "ping resource" must {
    "give a successful response" in {
      get("/ping").status mustBe OK
    }

    "give version information in the response body" in {
      (get("/ping").json \ "version").as[String] must not be empty
    }

    "give headers that disable caching of the response" in {
      get("/ping").header("Cache-Control").get mustBe "no-cache,max-age=0,must-revalidate"
    }

    "give a successful config response" in {
      get("/config").status mustBe OK
    }
  }

  "error resource" must {
    "give an error response and the server must still be running afterwards" in {
      get("/error").status mustBe INTERNAL_SERVER_ERROR
      get("/ping").status mustBe OK
    }
  }

  def appConfiguration: Map[String, String] = Map()
}
