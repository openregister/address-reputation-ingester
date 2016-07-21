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

package helper

import org.scalatest.Assertions
import play.api.Application
import play.api.libs.ws.{InMemoryBody, WS, WSRequestHolder, WSResponse}
import play.api.test.Helpers._

import scala.annotation.tailrec

trait AppServerTestApi extends Assertions {
  def appEndpoint: String

  def app: Application

  val defaultContentType = "text/plain; charset=UTF-8"

  def newRequest(method: String, path: String): WSRequestHolder = {
    WS.url(appEndpoint + path)(app).withMethod(method)
  }

  def newRequest(method: String, path: String, body: String, hdrs: (String, String)*): WSRequestHolder = {
    val wsBody = InMemoryBody(body.trim.getBytes("UTF-8"))
    newRequest(method, path).withHeaders(hdrs: _*).withBody(wsBody)
  }

  def request(method: String, p: String, hdrs: (String, String)*): WSResponse = {
    await(newRequest(method, p).withHeaders(hdrs: _*).execute())
  }

  def request(method: String, p: String): WSResponse = request(method, p, "User-Agent" -> "xyz")

  def get(p: String): WSResponse = request("GET", p)

  def verifyOK(path: String, expectedBody: String, expectedContent: String = "text/plain") {
    verify(path, OK, expectedBody, expectedContent)
  }

  def verify(path: String, expectedStatus: Int, expectedBody: String, expectedContent: String = "text/plain") {
    val step = get(path)
    assert(step.status === expectedStatus)
    assert(step.header("Content-Type") === Some(expectedContent))
    assert(step.body === expectedBody)
  }

  @tailrec
  final def waitWhile(path: String, currentBody: String, timeout: Int): Boolean = {
    if (timeout < 0) {
      false
    } else {
      Thread.sleep(200)
      val step = get(path)
      if (step.status != OK || step.body != currentBody) true
      else waitWhile(path, currentBody, timeout - 200)
    }
  }

  @tailrec
  final def waitUntil(path: String, currentBody: String, timeout: Int): Boolean = {
    if (timeout < 0) {
      false
    } else {
      Thread.sleep(200)
      val step = get(path)
      if (step.status == OK && step.body == currentBody) true
      else waitUntil(path, currentBody, timeout - 200)
    }
  }

  def dump(response: WSResponse) = {
    "\n  Got " + response.status + ":" + response.body
  }
}
