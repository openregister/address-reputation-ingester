/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package controllers

import config.ApplicationGlobal
import controllers.SimpleValidator._
import play.api.libs.json.Json
import play.api.mvc.{Action, ActionBuilder, AnyContent, Request}
import services.db.{CollectionMetadata, CollectionMetadataItem, CollectionName}
import services.exec.WorkerFactory
import services.model.StatusLogger
import uk.gov.hmrc.play.microservice.controller.BaseController


object CollectionController extends CollectionController(
  ControllerConfig.authAction,
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ApplicationGlobal.collectionMetadata,
  ApplicationGlobal.metadataStore
)


class CollectionController(action: ActionBuilder[Request],
                           status: StatusLogger,
                           workerFactory: WorkerFactory,
                           collectionMetadata: CollectionMetadata,
                           systemMetadata: SystemMetadataStore) extends BaseController {

  import CollectionInfo._

  def listCollections: Action[AnyContent] = action {
    request =>
      val pc = collectionsInUse
      val collections = collectionMetadata.existingCollectionMetadata
      val result =
        for (info <- collections) yield {
          val name = info.name.toString
          CollectionInfo(name, info.size,
            systemCollections.contains(name),
            pc.contains(name),
            info.createdAt.map(_.toString),
            info.completedAt.map(_.toString))
        }

      Ok(Json.toJson(ListCI(result)))
  }

  def dropCollection(name: String): Action[AnyContent] = action {
    request =>
      if (!isAlphaNumOrUnderscore(name))
        BadRequest(name)
      else if (!collectionMetadata.collectionExists(name)) {
        NotFound(name)
      } else if (systemCollections.contains(name) || collectionsInUse.contains(name)) {
        BadRequest(name + " cannot be dropped")
      } else {
        collectionMetadata.dropCollection(name)
        //TODO reverse routing via SeeOther(routes.CollectionController.listCollections())
        SeeOther("/collections/list")
      }
  }

  def doCleanup(): Action[AnyContent] = action {
    request =>
      workerFactory.worker.push("cleaning up obsolete collections", continuer => cleanup())
      Accepted
  }

  private[controllers] def cleanup() {
    val toGo = determineObsoleteCollections
    deleteObsoleteCollections(toGo)
  }

  private[controllers] def determineObsoleteCollections: Set[CollectionMetadataItem] = {
    // already sorted
    val collections: List[CollectionMetadataItem] = collectionMetadata.existingCollectionMetadata
    val mainCollections = collections.filterNot(cmi => systemCollections.contains(cmi.name.toString))

    // all incomplete collections are cullable
    val incompleteCollections = mainCollections.filter(_.isIncomplete)
    val completeCollections = mainCollections.filter(_.isComplete)

    val cullable: List[List[CollectionMetadataItem]] =
      for (product <- KnownProducts.OSGB) yield {
        // already sorted (still)
        val completeCollectionsForProduct: List[CollectionMetadataItem] = completeCollections.filter(_.name.productName == product)
        val inUse = CollectionName(collectionInUseFor(product)).get
        val i = completeCollectionsForProduct.indexWhere(_.name == inUse) - 1
        if (i < 0) {
          Nil
        } else {
          completeCollectionsForProduct.take(i)
        }
      }
    (incompleteCollections ++ cullable.flatten).toSet
  }

  private def deleteObsoleteCollections(unwantedCollections: Traversable[CollectionMetadataItem]) {
    for (col <- unwantedCollections) {
      val name = col.name.toString
      status.info(s"Deleting obsolete collection $name")
      collectionMetadata.dropCollection(name)
    }
  }

  private val systemCollections = Set("system.indexes", "admin")

  private def collectionInUseFor(product: String): String = systemMetadata.addressBaseCollectionItem(product).get

  private def collectionsInUse: Set[String] = KnownProducts.OSGB.map(n => collectionInUseFor(n)).toSet
}

