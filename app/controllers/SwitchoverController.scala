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

import config.ApplicationGlobal
import controllers.SimpleValidator._
import play.api.Logger
import play.api.mvc.{Action, AnyContent, Request, Result}
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.LoggerFacade
import uk.gov.hmrc.play.microservice.controller.BaseController


object SwitchoverController extends SwitchoverController(
  ApplicationGlobal.mongoConnection,
  new MetadataStoreFactory().newStore(ApplicationGlobal.mongoConnection)
)


class SwitchoverController(mongoDbConnection: CasbahMongoConnection, metadata: Map[String, StoredMetadataItem]) extends BaseController {

  def switchTo(product: String, epoch: String, index: String): Action[AnyContent] = Action {
    request =>
      handleSwitch(request, product, epoch, index)
  }

  private[controllers] def handleSwitch(request: Request[AnyContent],
                                        product: String, epoch: String, index: String): Result = {
    require(isAlphaNumeric(epoch))
    require(isNumeric(index))

    val addressBaseCollectionName: StoredMetadataItem = {
      val cn = metadata.get(product)
      if (cn.isEmpty) {
        throw new IllegalArgumentException("Unsupported product " + product)
      }
      cn.get
    }

    // this metadata key/value is checked by all address-lookup nodes once every few minutes
    val newName = s"${product}_${epoch}_${index}"

    val db = mongoDbConnection.getConfiguredDb
    if (!db.collectionExists(newName)) {
      BadRequest(s"$newName: collection was not found.").withHeaders(textPlain)
    }
    else if (db(newName).findOneByID("metadata").isEmpty) {
      Conflict(s"$newName: collection is still being written.").withHeaders(textPlain)
    }
    else {
      addressBaseCollectionName.set(newName)

      Ok(s"Switched over to $product/$epoch index $index.").withHeaders(textPlain)
    }
  }

  private val textPlain = CONTENT_TYPE -> "text/plain"
}


class MetadataStoreFactory {
  def newStore(mongo: CasbahMongoConnection): Map[String, StoredMetadataItem] = {
    val store = new MetadataStore(mongo, new LoggerFacade(Logger.logger))
    Map(
      "abp" -> store.gbAddressBaseCollectionName,
      "abi" -> store.niAddressBaseCollectionName
    )
  }
}
