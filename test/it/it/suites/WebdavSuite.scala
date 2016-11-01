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

class WebdavSuite(val appEndpoint: String, tmpDir: File)(implicit val app: Application)
  extends WordSpec with MustMatchers with AppServerTestApi {

  val idle = Synopsis.OkText("idle")

  "webdav fetch"  must {
    "get remote directory listing" in {
      val r =
        """http://localhost:8080/webdav
          |/
          |  webdav/
          |    exeter/
          |      1/
          |        full/
          |          data/
          |            addressbase-premium-csv-sample-data.zip            (data)       6715 KiB
          |          ready-to-collect.txt                               (txt)           0 KiB
          |  exeter/
          |    1/
          |      full/
          |        data/
          |          addressbase-premium-csv-sample-data.zip            (data)       6715 KiB
          |        ready-to-collect.txt                               (txt)           0 KiB
          |""".stripMargin

      val request = newRequest("GET", "/fetch/showRemoteTree")
      val response = await(request.withAuth("admin", "password", BASIC).execute())

      assert(response.body === r )
    }

    "retrieve a file from remote endpoint" in {
      // Have to use full as the WebDaveTree code expects only full
      val request = newRequest("GET", "/fetch/to/file/exeter/1/full?forceChange=true")
      val response = await(request.withAuth("admin", "password", BASIC).execute())

      assert(response.status === ACCEPTED)

      val busy = Synopsis.OkText("busy fetching exeter/1/full (forced)")
      assert(waitWhile("/admin/status", busy) === idle)

      val outputDir = new File(s"$tmpDir/download/exeter/1/full")
      val files = outputDir.listFiles()
      files.length mustBe 2 // zip & done
      val outFile = files.find(_.getName.endsWith(".zip")).get
      outFile.exists() mustBe true
      outFile.length() mustBe 6876716L
    }
  }

}
