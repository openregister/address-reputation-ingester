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
import db.CollectionMetadata
import play.api.Logger
import play.api.mvc.{Action, AnyContent}
import services.audit.AuditClient
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object SwitchoverController extends SwitchoverController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ApplicationGlobal.mongoConnection,
  ApplicationGlobal.metadataStore,
  services.audit.Services.auditClient
)


class SwitchoverController(status: StatusLogger,
                           workerFactory: WorkerFactory,
                           mongoDbConnection: CasbahMongoConnection,
                           systemMetadata: SystemMetadataStore,
                           auditClient: AuditClient) extends BaseController {

  def doSwitchTo(product: String, epoch: Int, index: Int): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      val model = new StateModel(product, epoch, None, Some(index))
      workerFactory.worker.push(s"switching to ${model.collectionName.toString}",         continuer =>          switchIfOK(model))
      Accepted
  }

  private[controllers] def switchIfOK(model: StateModel): StateModel = {
    if (!model.hasFailed) {
      switch(model)
    } else {
      status.info("Switchover was skipped.")
      model // unchanged
    }
  }

  private def switch(model: StateModel): StateModel = {

    if (model.index.isEmpty) {
      status.warn(s"cannot switch to ${model.collectionName.toPrefix} with unknown index")
      model.copy(hasFailed = true)

    } else {
      val newName = model.collectionName

      val db = mongoDbConnection.getConfiguredDb
      val collectionMetadata = new CollectionMetadata(db)
      if (!db.collectionExists(newName.toString)) {
        status.warn(s"$newName: collection was not found")
        model.copy(hasFailed = true)
      }
      else if (collectionMetadata.findMetadata(newName).exists(_.completedAt.isDefined)) {
        // this metadata key/value is checked by all address-lookup nodes once every few minutes
        setCollectionName(model.productName, model.epoch, newName.toString)
        status.info(s"Switched over to $newName")
        model // unchanged
      }
      else {
        status.warn(s"$newName: collection is still being written")
        model.copy(hasFailed = true)
      }
    }
  }

  private def setCollectionName(productName: String, epoch: Int, newName: String) {
    val addressBaseCollectionName = systemMetadata.addressBaseCollectionItem(productName)
    addressBaseCollectionName.set(newName)
    auditClient.succeeded(Map("product" -> productName, "epoch" -> epoch.toString, "newCollection" -> newName))
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
