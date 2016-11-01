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

import java.io.File

import it.helper.{AppServerTestApi, Synopsis}
import org.scalatest.{MustMatchers, WordSpec}
import play.api.Application
import play.api.libs.ws.WSAuthScheme.BASIC
import play.api.test.Helpers._

class IngestFileSuite(val appEndpoint: String, tmpDir: File)(implicit val app: Application)
  extends WordSpec with MustMatchers with AppServerTestApi {

  val idle = Synopsis.OkText("idle")

  "ingest resource happy journey - to file" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await successful outcome,
       * observe quiet status
    """ in {
      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/file/exeter/1/sample?forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to file exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val outputDir = new File(tmpDir, "output")
      val files = outputDir.listFiles()
      files.length mustBe 1
      val outFile = files.head
      outFile.exists() mustBe true
      outFile.length() mustBe 1109057L
    }
  }

}
