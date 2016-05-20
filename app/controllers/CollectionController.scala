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
import ingest.writers.{CollectionMetadata, CollectionMetadataItem, CollectionName}
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.play.microservice.controller.BaseController


object CollectionController extends CollectionController(
  new WorkerFactory,
  ApplicationGlobal.mongoConnection,
  ApplicationGlobal.metadataStore
)


class CollectionController(workerFactory: WorkerFactory,
                           mongoDbConnection: CasbahMongoConnection,
                           systemMetadata: SystemMetadataStore) extends BaseController {

  import CollectionInfo._

  private lazy val db = mongoDbConnection.getConfiguredDb
  private lazy val collectionMetadata = new CollectionMetadata(db)

  def listCollections: Action[AnyContent] = Action {
    request =>
      val pc = collectionsInUse
      val collections = collectionMetadata.existingCollectionMetadata
      val result =
        for (info <- collections) yield {
          val name = info.name.toString
          val size = db(name).size
          CollectionInfo(name, size,
            systemCollections.contains(name),
            pc.contains(name),
            info.createdAt.map(_.toString),
            info.completedAt.map(_.toString))
        }

      Ok(Json.toJson(ListCI(result)))
  }

  def dropCollection(name: String): Action[AnyContent] = Action {
    request =>
      if (!isAlphaNumOrUnderscore(name))
        BadRequest(name)
      else if (!db.collectionExists(name)) {
        NotFound
      } else if (systemCollections.contains(name) || collectionsInUse.contains(name)) {
        BadRequest(name + " cannot be dropped")
      } else {
        db(name).dropCollection()
        //TODO reverse routing via SeeOther(routes.CollectionController.listCollections())
        SeeOther("/collections/list")
      }
  }

  def doCleanup(): Action[AnyContent] = Action {
    request =>
      workerFactory.worker.push("cleaning up obsolete collections", {
        continuer =>
          val toGo = determineObsoleteCollections
          deleteObsoleteCollections(toGo)
      })
      Accepted
  }

  private[controllers] def determineObsoleteCollections: Set[CollectionName] = {
    // already sorted
    val collections: List[CollectionMetadataItem] = collectionMetadata.existingCollectionMetadata
    val mainCollections = collections.filterNot(cmi => systemCollections.contains(cmi.name.toString))

    // all incomplete collections are cullable
    val incompleteCollections = mainCollections.filter(_.isIncomplete).map(_.name)
    val completeCollections = mainCollections.filter(_.isComplete).map(_.name)

    val cullable: List[List[CollectionName]] =
      for (product <- KnownProducts.OSGB) yield {
        // already sorted (still)
        val completeCollectionsForProduct: List[CollectionName] = completeCollections.filter(_.productName == product)
        val inUse = CollectionName(collectionInUseFor(product)).get
        val i = completeCollectionsForProduct.indexOf(inUse) - 1
        if (i < 0) {
          Nil
        } else {
          completeCollectionsForProduct.take(i)
        }
      }
    (incompleteCollections ++ cullable.flatten).toSet
  }

  private def deleteObsoleteCollections(unwantedCollections: Traversable[CollectionName]) {
    for (col <- unwantedCollections) {
      db(col.toString).drop()
    }
  }

  private val systemCollections = Set("system.indexes", "admin")

  private def collectionInUseFor(product: String): String = systemMetadata.addressBaseCollectionItem(product).get

  private def collectionsInUse: Set[String] = KnownProducts.OSGB.map(n => collectionInUseFor(n)).toSet
}

