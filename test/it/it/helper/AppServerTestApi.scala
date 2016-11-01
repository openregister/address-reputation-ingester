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

import java.util.concurrent.TimeUnit

import akka.util.ByteString
import org.scalatest.Assertions
import play.api.Application
import play.api.libs.ws.{InMemoryBody, WS, WSRequest, WSResponse}
import play.api.test.Helpers._

import scala.annotation.tailrec
import scala.concurrent.duration.Duration

trait AppServerTestApi extends Assertions {
  def appEndpoint: String

  def app: Application

  val textPlainUTF8 = "text/plain; charset=UTF-8"

  //-----------------------------------------------------------------------------------------------

  def newRequest(method: String, path: String): WSRequest = {
    WS.url(appEndpoint + path)(app).withMethod(method).withRequestTimeout(Duration(60, TimeUnit.SECONDS))
  }

  def newRequest(method: String, path: String, body: String): WSRequest = {
    val wsBody = InMemoryBody(ByteString(body.trim))
    newRequest(method, path).withHeaders("Content-Type" -> "application/json").withBody(wsBody)
  }

  //-----------------------------------------------------------------------------------------------

  def request(method: String, path: String, hdrs: (String, String)*): WSResponse =
    await(newRequest(method, path).withHeaders(hdrs: _*).execute())

  def request(method: String, path: String, body: String, hdrs: (String, String)*): WSResponse =
    await(newRequest(method, path, body).withHeaders(hdrs: _*).execute())

  def get(path: String): WSResponse =
    await(newRequest("GET", path).withHeaders("User-Agent" -> "xyz").execute())

  def delete(path: String): WSResponse =
    await(newRequest("DELETE", path).withHeaders("User-Agent" -> "xyz").execute())

  def post(path: String, body: String, ct: String = "application/json"): WSResponse =
    await(newRequest("POST", path, body).withHeaders("Content-Type" -> ct, "User-Agent" -> "xyz").execute())

  def put(path: String, body: String, ct: String = "application/json"): WSResponse =
    await(newRequest("PUT", path, body).withHeaders("Content-Type" -> ct, "User-Agent" -> "xyz").execute())

  //-----------------------------------------------------------------------------------------------

  val defaultTimeout = 600000

  @tailrec
  final def waitWhile(path: String, current: Synopsis, timeout: Int = defaultTimeout): Synopsis = {
    if (timeout < 0) {
      println("Timed out")
      Synopsis.empty
    } else {
      if (timeout % 10000 == 0) println(s"Waiting on $path while " + current.body)
      Thread.sleep(200)
      val step = Synopsis(get(path))
      if (step != current) step
      else waitWhile(path, current, timeout - 200)
    }
  }

  @tailrec
  final def waitUntil(path: String, current: Synopsis, timeout: Int = defaultTimeout): Synopsis = {
    if (timeout < 0) {
      println("Timed out")
      Synopsis.empty
    } else {
      if (timeout % 10000 == 0) println(s"Waiting on $path until " + current.body)
      Thread.sleep(200)
      val step = Synopsis(get(path))
      if (step == current) step
      else waitUntil(path, current, timeout - 200)
    }
  }

  // provides a lazy wrapper containing a toString method
  // (normally there is no need to dump the response)
  def dump(response: WSResponse): WSResponseDumper = new WSResponseDumper(response)
}

