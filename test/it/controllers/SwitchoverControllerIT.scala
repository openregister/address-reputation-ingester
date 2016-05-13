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

import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import helper.{AppServerUnderTest, EmbeddedMongoSuite}
import org.scalatestplus.play.PlaySpec
import play.api.test.Helpers._
import services.writers.CollectionMetadata
import uk.co.hmrc.address.admin.MetadataStore
import uk.co.hmrc.logging.Stdout

class SwitchoverControllerIT extends PlaySpec with EmbeddedMongoSuite with AppServerUnderTest {

  def appConfiguration: Map[String, String] = Map()

  "switch-over resource error journeys" must {
    """
       * attempt to switch to non-existent collection
       * should not change the nominated collection
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get

      assert(get("/switch/to/abp/39/3").status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }

    """
       * attempt to switch to existing collection that has no completedAt metadata
       * should not change the nominated collection
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      val initialCollectionName = admin.gbAddressBaseCollectionName.get
      CollectionMetadata.writeCreationDateTo(mongo.getConfiguredDb("abp_39_004"))

      assert(get("/switch/to/abp/39/4").status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === initialCollectionName)

      mongo.close()
    }
  }

  "switch-over resource happy journey" must {
    """
       * attempt to switch to existing collection that has completedAt metadata
       * should change the nominated collection
    """ in {
      val mongo = casbahMongoConnection()
      val admin = new MetadataStore(mongo, Stdout)
      CollectionMetadata.writeCreationDateTo(mongo.getConfiguredDb("abp_39_005"))
      CollectionMetadata.writeCompletionDateTo(mongo.getConfiguredDb("abp_39_005"))

      assert(get("/switch/to/abp/39/5").status === ACCEPTED)
      assert(waitUntil("/admin/status", "idle", 100000) === true)

      val collectionName = admin.gbAddressBaseCollectionName.get
      assert(collectionName === "abp_39_005")

      mongo.close()
    }
  }

}
