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

import controllers.SimpleValidator._
import fetch.{FetchController, FetchControllerHelper, SardineWrapper, WriterSettings}
import play.api.mvc.{Action, AnyContent}
import services.exec.{Continuer, WorkerFactory}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController

object KnownProducts {
  val OSGB = List("abi", "abp")
}


object GoController extends GoController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.sardine,
  FetchController,
  SwitchoverController,
  CollectionController
)


class GoController(logger: StatusLogger,
                   workerFactory: WorkerFactory,
                   sardine: SardineWrapper,
                   fetchController: FetchController,
                   switchoverController: SwitchoverController,
                   collectionController: CollectionController) extends BaseController {

  def doGoAuto(bulkSize: Option[Int], loopDelay: Option[Int]): Action[AnyContent] = Action {
    request =>

      val settings = FetchControllerHelper.settings(bulkSize, loopDelay)
      workerFactory.worker.push(s"automatically searching", {
        continuer =>
          val tree = sardine.exploreRemoteTree
          logger.info(tree.toString)
          for (product <- KnownProducts.OSGB
               if continuer.isBusy) {
            val found = tree.findLatestFor(product)
            if (found.isDefined) {
              val model = StateModel(found.get)
              pipeline(model, settings, continuer)
            }
          }
          if (continuer.isBusy) {
            collectionController.cleanup()
          }
      })
      Accepted
  }

  def doGo(product: String, epoch: Int, variant: String,
           bulkSize: Option[Int], loopDelay: Option[Int],
           forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = FetchControllerHelper.settings(bulkSize, loopDelay)
      val model = new StateModel(product, epoch, Some(variant), forceChange = forceChange getOrElse false)
      val worker = workerFactory.worker
      worker.push(s"automatically loading ${model.pathSegment}", {
        continuer =>
          pipeline(model, settings, continuer)
      })
      Accepted
  }

  private def pipeline(model1: StateModel, settings: WriterSettings, continuer: Continuer) {
    if (continuer.isBusy) {
      fetchController.fetch(model1, settings, continuer)
    }
  }
}
