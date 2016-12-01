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

import java.io._
import java.util.zip.GZIPInputStream

import it.helper.{AppServerTestApi, Synopsis}
import org.scalatest.{MustMatchers, WordSpec}
import play.api.Application
import play.api.test.Helpers._

import scala.io.Source

class IngestFileSuite(val appEndpoint: String, tmpDir: File)(implicit val app: Application)
  extends WordSpec with MustMatchers with AppServerTestApi {

  val idle = Synopsis.OkText("idle")

  "ingest resource happy journey - to file using DPA+LPI" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await successful outcome,
       * observe quiet status
       * set the filename based on the settings
    """ in {
      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/file/exeter/1/sample?forceChange=true&prefer=DPA&include=DPA+LPI&streetFilter=1")
      val response = await(request.execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to file exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val outputDir = new File(tmpDir, "output")
      val files = outputDir.listFiles()
//      println(files.toList)
      val chosen = files.filter(_.getName.contains("-DPA+LPI-1."))
      chosen.length mustBe 1
      val outFile = chosen.head
      outFile.exists() must be(true)
      outFile.length() must be(1109057L)
      numLines(outFile) must be(48737)
    }
  }

  "ingest resource happy journey - to file using LPI+DPA" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await successful outcome,
       * observe quiet status
       * set the filename based on the settings
    """ in {
      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/file/exeter/1/sample?forceChange=true&prefer=LPI&include=LPI+DPA&streetFilter=2")
      val response = await(request.execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to file exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val outputDir = new File(tmpDir, "output")
      val files = outputDir.listFiles()
//      println(files.toList)
      val chosen = files.filter(_.getName.contains("-LPI+DPA-2."))
      chosen.length mustBe 1
      val outFile = chosen.head
      outFile.exists() must be(true)
      outFile.length() must be(836132L)
      numLines(outFile) must be(48737)
    }
  }

  "ingest resource happy journey - to file using LPI only" must {
    """
       * observe quiet status,
       * start ingest,
       * observe busy status,
       * await successful outcome,
       * observe quiet status
       * set the filename based on the settings
    """ in {
      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/ingest/from/file/to/file/exeter/1/sample?forceChange=true&prefer=LPI&include=LPI&streetFilter=0")
      val response = await(request.execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy ingesting to file exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val outputDir = new File(tmpDir, "output")
      val files = outputDir.listFiles()
//      println(files.toList)
      val chosen = files.filter(_.getName.contains("-LPI-0."))
      chosen.length mustBe 1
      val outFile = chosen.head
      outFile.exists() must be(true)
      outFile.length() must be(836180L)
      numLines(outFile) must be(48737)
    }
  }

  private def numLines(file: File) = {
    val source = Source.fromInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(file))), "UTF-8")
    var n = 0
    source.getLines.foreach { s => n += 1 }
    n
  }
}
