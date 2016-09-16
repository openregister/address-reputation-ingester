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

package services.mongo

import play.api.Logger
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

class MongoSystemMetadataStoreFactory {
  def newStore(mongo: CasbahMongoConnection): MongoSystemMetadataStore =
    new MongoSystemMetadataStore(mongo, new LoggerFacade(Logger.logger))
}


class MongoSystemMetadataStore(mongo: CasbahMongoConnection, logger: SimpleLogger) {
  private val store = new MetadataStore(mongo, logger)
  private val table = Map(
    "abp" -> store.gbAddressBaseCollectionName,
    "abi" -> store.niAddressBaseCollectionName
  )

  def addressBaseCollectionItem(productName: String): StoredMetadataItem = {
    val cn = table.get(productName)
    if (cn.isEmpty) {
      throw new IllegalArgumentException(s"Unsupported product $productName")
    }
    cn.get
  }
}
