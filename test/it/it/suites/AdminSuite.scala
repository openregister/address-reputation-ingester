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

package it.suites

import it.helper.AppServerTestApi
import org.scalatest.{DoNotDiscover, MustMatchers, WordSpec}
import play.api.Application
import play.api.test.Helpers._

@DoNotDiscover
class AdminSuite()(implicit val app: Application, implicit val appEndpoint: String)
  extends WordSpec with MustMatchers with AppServerTestApi {

  // only light-weight admin tests are provided; mostly, manually testing happens here
  "admin endpoints should not cause error" must {
    "status" in {
      // already covered elsewhere
    }

    "fullStatus" in {
      val request = newRequest("GET", "/admin/fullStatus")
      val response = await(request.execute())
      assert(response.status === OK)
      assert(response.body.nonEmpty)
    }

    "cancelCurrent" in {
      val request = newRequest("GET", "/admin/cancelCurrent")
      val response = await(request.execute())
      assert(response.status === OK) // when not busy
    }

    "cancel" in {
      val request = newRequest("GET", "/admin/cancel/0")
      val response = await(request.execute())
      assert(response.status === OK) // when not busy
    }

    "dirTree" in {
      val request = newRequest("GET", "/admin/dirTree")
      val response = await(request.execute())
      assert(response.status === OK)
      assert(response.body.nonEmpty)
    }
  }
}
