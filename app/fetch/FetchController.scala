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
 */

package fetch

import java.io.File

import com.google.inject.Inject
import controllers.KnownProducts
import controllers.SimpleValidator._
import play.api.mvc.{Action, AnyContent}
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.es.{IndexMetadata, IndexName}
import uk.gov.hmrc.play.microservice.controller.BaseController
import uk.gov.hmrc.util.FileUtils


class FetchController @Inject() (logger: StatusLogger,
                      worker: WorkQueue,
                      webdavFetcher: WebdavFetcher,
                      sardine: SardineWrapper,
                      indexMetadata: IndexMetadata) extends BaseController {

  def doFetchToFile(product: String, epoch: Int, variant: String, forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val model = new StateModel(product, Some(epoch), Some(variant), forceChange = forceChange getOrElse false)
      worker.push(s"fetching ${model.pathSegment}${model.forceChangeString}", continuer => fetch(model, continuer))
      Accepted(s"Fetch has started for ${model.pathSegment}${model.forceChangeString}")
  }

  def fetch(model1: StateModel, continuer: Continuer): StateModel = {
    val model2 =
      if (model1.product.isDefined) model1
      else {
        val tree = sardine.exploreRemoteTree
        val found = tree.findAvailableFor(model1.productName, model1.epoch.get)
        model1.copy(product = found)
      }

    val files =
      if (model2.product.isDefined)
        webdavFetcher.fetchList(model2.product.get, model2.pathSegment, continuer, model2.forceChange)
      else {
        logger.info("Nothing new available for " + model1.pathSegment)
        Nil
      }

    if (files.isEmpty) model2.copy(hasFailed = true) else model2
  }

  def doCleanup(): Action[AnyContent] = Action {
    request =>
      worker.push(s"zip file cleanup", {
        continuer =>
          cleanup()
      })
      Accepted("ok")
  }

  def cleanup() {
    val unwanted = determineObsoleteFiles(KnownProducts.OSGB)
    for (dir <- unwanted) {
      logger.info(s"Deleting ${dir.getPath}/...")
      FileUtils.deleteDir(dir)
    }
  }

  def doShowTree(): Action[AnyContent] = Action {
    val tree = sardine.exploreRemoteTree
    Ok(sardine.url + "\n" + tree.indentedString)
  }

  private[fetch] def determineObsoleteFiles(products: List[String]): List[File] = {
    // already sorted
    val existingIndexes: List[IndexName] = indexMetadata.existingIndexes
    val files = webdavFetcher.downloadFolder.listFiles
    val productDirs: List[File] = if (files == null) List.empty else files.toList
    val epochDirs: List[File] = productDirs.flatMap(_.listFiles)
    val filtered = for (p <- products) yield {
      determineObsoleteFilesFor(p, existingIndexes, productDirs)
    }
    filtered.flatten.sorted
  }

  private def determineObsoleteFilesFor(product: String, existingIndexes: List[IndexName], productDirs: List[File]): List[File] = {
    val relevantIndexes = existingIndexes.filter(_.productName == product)
    val relevantEpochs: List[Int] = relevantIndexes.flatMap(_.epoch)
    val relevantDirs = productDirs.filter(_.getName == product)
    val epochDirs: List[File] = relevantDirs.flatMap(_.listFiles)
    epochDirs.filterNot {
      dir =>
        val name = dir.getName
        if (numeric.matcher(name).matches) {
          val epoch = name.toInt
          relevantEpochs.contains(epoch) || epoch > relevantEpochs.last
        } else false
    }
  }

  private val numeric = "\\d+".r.pattern
}
