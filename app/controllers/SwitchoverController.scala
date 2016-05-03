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
import play.api.Logger
import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.WorkerFactory
import services.ingester.model.StateModel
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object SwitchoverController extends SwitchoverController(
  new WorkerFactory,
  new LoggerFacade(Logger.logger),
  ApplicationGlobal.mongoConnection,
  new MetadataStoreFactory().newStore(ApplicationGlobal.mongoConnection)
)


class SwitchoverController(workerFactory: WorkerFactory,
                           logger: SimpleLogger,
                           mongoDbConnection: CasbahMongoConnection,
                           metadata: Map[String, StoredMetadataItem]) extends BaseController {

  def switchTo(product: String, epoch: Int, index: Int): Action[AnyContent] = Action {
    request =>
      val model = new StateModel(logger, product, epoch, "", Some(index))
      queueSwitch(model)
      Accepted
  }

  private[controllers] def queueSwitch(model: StateModel) {
    workerFactory.worker.push(
      s"switching to ${model.collectionName.get}", model, {
        continuer =>
          if (!model.hasFailed) {
            switch(model)
          } else {
            model.statusLogger.info("Switchover was skipped.")
          }
      })
  }

  private[controllers] def switch(model: StateModel) {

    val addressBaseCollectionName: StoredMetadataItem = {
      val cn = metadata.get(model.product)
      if (cn.isEmpty) {
        throw new IllegalArgumentException(s"Unsupported product ${model.product}")
      }
      cn.get
    }

    // this metadata key/value is checked by all address-lookup nodes once every few minutes
    val newName = model.collectionName.get

    val db = mongoDbConnection.getConfiguredDb
    if (!db.collectionExists(newName)) {
      model.fail(s"$newName: collection was not found")
    }
    else if (db(newName).findOneByID("metadata").isEmpty) {
      model.fail(s"$newName: collection is still being written")
    }
    else {
      addressBaseCollectionName.set(newName)
      model.statusLogger.info(s"Switched over to $newName")
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
