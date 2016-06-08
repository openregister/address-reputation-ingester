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

import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec
import play.api.test.Helpers._
import ingest.writers.CollectionMetadata
import uk.co.hmrc.address.admin.MetadataStore
import uk.co.hmrc.logging.Stdout

class CollectionControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  def appConfiguration: Map[String, String] = Map()

  "list collections" must {
    """
       * return the sorted list of collections
       * along with the completion dates (if present)
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      CollectionMetadata.writeCompletionDateTo(mongo.getConfiguredDb("abp_39_5"))

      val response = get("/collections/list")
      assert(response.status === OK)
      //      assert(response.body === "foo")

      assert(waitUntil("/admin/status", "idle", 100000) === true)
      mongo.close()
    }
  }

}

