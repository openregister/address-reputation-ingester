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

package controllers

import com.google.inject.Inject
import play.api.libs.json.Json
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkQueue
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es.{IndexMetadata, IndexMetadataItem, IndexName}
import uk.gov.hmrc.play.microservice.controller.BaseController


class IndexController @Inject()(status: StatusLogger,
                                worker: WorkQueue,
                                indexMetadata: IndexMetadata) extends BaseController {

  import IndexInfo._

  def doListIndexes(): Action[AnyContent] = Action { request =>
    val result = listIndexes()
    Ok(Json.toJson(ListCI(result)))
  }

  private def listIndexes(): List[IndexInfo] = {
    val pc = indexesInUse
    val indexes = indexMetadata.existingIndexMetadata
    for (info <- indexes) yield {
      val name = info.name.toString
      IndexInfo(
        name = name,
        size = info.size,
        system = systemIndexes.contains(name),
        inUse = pc.contains(name),
        createdAt = info.createdAt.map(_.toString),
        completedAt = info.completedAt.map(_.toString),
        bulkSize = info.bulkSize,
        loopDelay = info.loopDelay,
        includeDPA = info.includeDPA,
        includeLPI = info.includeLPI,
        prefer = info.prefer,
        streetFilter = info.streetFilter,
        buildVersion = info.buildVersion,
        buildNumber = info.buildNumber,
        aliases = info.aliases
      )
    }
  }

  def doDeleteIndex(name: String): Action[AnyContent] = Action {
    request =>
      val cn = IndexName(name)
      if (cn.isEmpty)
        BadRequest(name)
      else if (!indexMetadata.indexExists(cn.get)) {
        NotFound(name)
      } else if (systemIndexes.contains(name) || indexesInUse.contains(name)) {
        BadRequest(name + " cannot be dropped")
      } else {
        worker.push("dropping index " + name, continuer => {
          indexMetadata.deleteIndex(cn.get)
        })
        Accepted
      }
  }

  def doCleanup(): Action[AnyContent] = Action {
    request =>
      worker.push("cleaning up obsolete indexes", continuer => cleanup())
      Accepted
  }

  private[controllers] def cleanup() {
    val toGo = determineObsoleteIndexes
    deleteObsoleteIndexes(toGo)
  }

  private[controllers] def determineObsoleteIndexes: Set[IndexMetadataItem] = {
    // already sorted
    val indexes: List[IndexMetadataItem] = indexMetadata.existingIndexMetadata
    val mainIndexes = indexes.filterNot(cmi => systemIndexes.contains(cmi.name.toString))

    // all incomplete indexes are cullable
    val incompleteIndexes = mainIndexes.filter(_.isIncomplete)
    val completeIndexes = mainIndexes.filter(_.isComplete)

    val cullable: List[List[IndexMetadataItem]] =
      for (product <- KnownProducts.OSGB) yield {
        // already sorted (still)
        val completeIndexesForProduct: List[IndexMetadataItem] = completeIndexes.filter(_.name.productName == product)
        val inUse = indexMetadata.getIndexNameInUseFor(product)
        val i = completeIndexesForProduct.indexWhere(c => inUse.contains(c.name)) - 1
        if (i < 0) {
          Nil
        } else {
          completeIndexesForProduct.take(i)
        }
      }
    (incompleteIndexes ++ cullable.flatten).toSet
  }

  private def deleteObsoleteIndexes(unwantedIndexes: Traversable[IndexMetadataItem]) {
    for (col <- unwantedIndexes) {
      val name = col.name.toString
      status.info(s"Deleting obsolete index $name")
      indexMetadata.deleteIndex(col.name)
    }
  }

  private val systemIndexes = Set("system.indexes", "admin")

  private def indexesInUse: Set[String] =
    KnownProducts.OSGB.flatMap(n => indexMetadata.getIndexNameInUseFor(n)).map(_.toString).toSet
}

