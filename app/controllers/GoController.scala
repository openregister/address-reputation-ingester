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

import com.google.inject.Inject
import controllers.SimpleValidator._
import fetch.{FetchController, SardineWrapper}
import ingest.{IngestController, IngestControllerHelper}
import play.api.mvc.{Action, AnyContent}
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.es.IndexMetadata
import uk.gov.hmrc.address.services.writers.{Algorithm, WriterSettings}
import uk.gov.hmrc.play.microservice.controller.BaseController

object KnownProducts {
  val OSGB = List("abi", "abp")
}


class GoController @Inject()(logger: StatusLogger,
                             worker: WorkQueue,
                             sardine: SardineWrapper,
                             fetchController: FetchController,
                             ingestController: IngestController,
                             esSwitchoverController: SwitchoverController,
                             esIndexController: IndexController,
                             indexMetadata: IndexMetadata) extends BaseController {

  def doGoAuto(target: String,
               bulkSize: Option[Int], loopDelay: Option[Int]): Action[AnyContent] = Action {
    require(IngestControllerHelper.allowedTargets.contains(target))

    val settings = IngestControllerHelper.settings(bulkSize, loopDelay, Algorithm.default)
    worker.push(s"automatically searching and loading to $target", {
      continuer =>
        val tree = sardine.exploreRemoteTree
        for (product <- KnownProducts.OSGB
             if continuer.isBusy) {
          val found = tree.findLatestFor(product)
          if (found.isDefined) {
            val model = StateModel(found.get)
            pipeline(target, model, settings, continuer)
          }
        }
        if (continuer.isBusy) {
          target match {
            case "es" => esIndexController.cleanup()
            case _ => // no action
          }
          fetchController.cleanup()
        }
    })
    Accepted
  }

  def doGo(target: String, product: String, epoch: Int, variant: String,
           bulkSize: Option[Int], loopDelay: Option[Int],
           forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.allowedTargets.contains(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = IngestControllerHelper.settings(bulkSize, loopDelay, Algorithm.default)
      val model = new StateModel(product, Some(epoch), Some(variant), forceChange = forceChange getOrElse false)
      worker.push(s"automatically loading to $target ${model.pathSegment}${model.forceChangeString}", {
        continuer =>
          pipeline(target, model, settings, continuer)
      })
      Accepted
  }

  private def pipeline(target: String, model1: StateModel, settings: WriterSettings, continuer: Continuer) {
    if (shouldIngest(model1, continuer)) {
      val model2 = fetchController.fetch(model1, continuer)
      val model3 = ingestController.ingestIfOK(model2, logger, settings, target, continuer)
      target match {
        case "es" => esSwitchoverController.switchIfOK(model3)
        case _ => // no further action
      }
    }
  }

  def shouldIngest(model1: StateModel, continuer: Continuer): Boolean = {
    val prod = model1.productName
    val epoch = model1.epoch
    val exists = indexExists(prod, epoch)
    val force = model1.forceChange
    val should = continuer.isBusy && (!exists || force)
    if (!should) {
      logger.info(s"Skipping ingest: index already exists in ES for $prod ${epoch.getOrElse(-1)}")
    }
    should
  }

  def indexExists(product: String, epoch: Option[Int]): Boolean = {
    (product, epoch) match {
      case (prod, Some(ep)) => indexMetadata.getIndexNameInUseFor(prod).exists(_.epoch == Some(ep))
      case _ => false
    }
  }

}
