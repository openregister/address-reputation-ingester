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
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import services.writers.CollectionMetadata
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object SwitchoverController extends SwitchoverController(
  new WorkerFactory,
  ApplicationGlobal.mongoConnection,
  ApplicationGlobal.metadataStore
)


class SwitchoverController(workerFactory: WorkerFactory,
                           mongoDbConnection: CasbahMongoConnection,
                           systemMetadata: SystemMetadataStore) extends BaseController {

  def doSwitchTo(product: String, epoch: Int, index: Int): Action[AnyContent] = Action {
    request =>
      val model = new StateModel(product, epoch, "", Some(index))
      workerFactory.worker.push(s"switching to ${model.collectionBaseName} ${model.index.get}", {
        continuer =>
          switchIfOK(model, workerFactory.worker.statusLogger)
      })
      Accepted
  }

  private[controllers] def switchIfOK(model: StateModel, status: StatusLogger): StateModel = {
    if (!model.hasFailed) {
      switch(model, status)
    } else {
      status.info("Switchover was skipped.")
      model // unchanged
    }
  }

  private def switch(model: StateModel, status: StatusLogger): StateModel = {

    val addressBaseCollectionName = systemMetadata.addressBaseCollectionItem(model.productName)

    // this metadata key/value is checked by all address-lookup nodes once every few minutes
    val newName = CollectionMetadata.formatName(model.collectionBaseName, model.index.get)

    val db = mongoDbConnection.getConfiguredDb
    if (!db.collectionExists(newName)) {
      status.warn(s"$newName: collection was not found")
      model.copy(hasFailed = true)
    }
    else if (db(newName).findOneByID("metadata").isEmpty) {
      status.warn(s"$newName: collection is still being written")
      model.copy(hasFailed = true)
    }
    else {
      addressBaseCollectionName.set(newName)
      status.info(s"Switched over to $newName")
      model // unchanged
    }
  }
}


class SystemMetadataStoreFactory {
  def newStore(mongo: CasbahMongoConnection): SystemMetadataStore =
    new SystemMetadataStore(mongo, new LoggerFacade(Logger.logger))
}


class SystemMetadataStore(mongo: CasbahMongoConnection, logger: SimpleLogger) {
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
