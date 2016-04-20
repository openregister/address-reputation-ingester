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

import play.api.test.Helpers._
import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec

class SwitchoverControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  def appConfiguration: Map[String, String] = Map()

  "switch-over resource happy journey - to file" must {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await termination
       * observe quiet status
    """ in {
      waitWhile("/admin/status", "busy ingesting")

      verifyOK("/admin/status", "idle")

      val step2 = get("/switch/to/abp/39/3")
      assert(step2.status === OK)

      //      verifyOK("/admin/status", "busy ingesting")
      //
      //      waitWhile("/admin/status", "busy ingesting")
      //
      //      verifyOK("/admin/status", "idle")
      //
      //      val outFile = new File("/var/tmp/abp_123456.txt.gz")
      //      outFile.exists() mustBe true
    }
  }

}
