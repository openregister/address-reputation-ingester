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

import play.api.http.Status
import play.api.libs.ws.WSResponse

/** Holds a summary of a response, for easy comparison. */
case class Synopsis(status: Int, body: String, content: Option[String] = Some("text/plain"))


object Synopsis {
  val empty = Synopsis(0, "", None)

  def apply(res: WSResponse): Synopsis = Synopsis(res.status, res.body, res.header("Content-Type"))

  def OkText(body: String, content: Option[String] = Some("text/plain")): Synopsis = Synopsis(Status.OK, body, content)
}


class WSResponseDumper(response: WSResponse) {
  override def toString: String = "\n  Got " + response.status + ":" + response.body
}
