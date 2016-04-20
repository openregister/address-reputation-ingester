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

import com.mongodb.casbah.commons.MongoDBObject
import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec
import play.api.test.Helpers._
import uk.co.hmrc.address.admin.MetadataStore
import uk.co.hmrc.logging.Stdout

class SwitchoverControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  def appConfiguration: Map[String, String] = Map()

  "switch-over resource error journeys" must {
    """
       * attempt to switch to non-existent collection
       * receive BadRequest response
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get

      verify("/switch/to/abp/39/3", BAD_REQUEST, "abp_39_3: collection was not found.")

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }

    """
       * attempt to switch to existing collection that is without completion metadata
       * receive Conflict response
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get
      mongo.getConfiguredDb("abp_39_4").insert(MongoDBObject("_id" -> "foo", "bar" -> true))

      verify("/switch/to/abp/39/4", CONFLICT, "abp_39_4: collection is still being written.")

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }
  }

  "switch-over resource happy journey" must {
    """
       * attempt to switch to existing collection that is without completion metadata
       * receive OK response
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get
      mongo.getConfiguredDb("abp_39_5").insert(MongoDBObject("_id" -> "metadata", "completedAt" -> "some date"))

      verify("/switch/to/abp/39/5", OK, "Switched over to abp/39 index 5.")

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === "abp_39_5")

      mongo.close()
    }
  }

}
