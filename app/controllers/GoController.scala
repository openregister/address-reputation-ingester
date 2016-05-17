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
import fetch.{FetchController, SardineWrapper}
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import services.writers.WriterSettings
import uk.co.hmrc.logging.SimpleLogger
import uk.gov.hmrc.play.microservice.controller.BaseController

object KnownProducts {
  val OSGB = List("abi", "abp")
}


object GoController extends GoController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.sardine,
  FetchController,
  IngestController,
  SwitchoverController
)


class GoController(logger: StatusLogger,
                   workerFactory: WorkerFactory,
                   sardine: SardineWrapper,
                   fetchController: FetchController,
                   ingestController: IngestController,
                   switchoverController: SwitchoverController) extends BaseController {

  def doGoAuto(target: String,
               bulkSize: Option[Int], loopDelay: Option[Int]): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))

      val settings = IngestControllerHelper.settings(bulkSize, loopDelay)
      workerFactory.worker.push(s"automatically searching", {
        continuer =>
          val tree = sardine.exploreRemoteTree
          logger.info(tree.toString)
          for (product <- KnownProducts.OSGB) {
            val found = tree.findLatestFor(product)
            if (found.isDefined) {
              val model = StateModel(found.get)
              pipeline(target, model, settings)
            }
          }
      })
      Accepted
  }

  def doGo(target: String, product: String, epoch: Int, variant: String,
           bulkSize: Option[Int], loopDelay: Option[Int],
           forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = IngestControllerHelper.settings(bulkSize, loopDelay)
      val model = new StateModel(product, epoch, Some(variant), forceChange = forceChange getOrElse false)
      pipeline(target, model, settings)
      Accepted
  }

  private def pipeline(target: String, model1: StateModel, settings: WriterSettings) {
    val worker = workerFactory.worker
    worker.push(s"automatically loading ${model1.pathSegment}", {
      continuer =>
        if (continuer.isBusy) {
          val model2 = fetchController.fetch(model1)
          val model3 = ingestController.ingestIfOK(model2, worker.statusLogger, settings, target, continuer)
          if (target == "db") {
            switchoverController.switchIfOK(model3, worker.statusLogger)
          }
        }
    })
  }

}
