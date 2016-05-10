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
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.fetch.SardineWrapper
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


class GoController(logger: SimpleLogger,
                   workerFactory: WorkerFactory,
                   sardine: SardineWrapper,
                   fetchController: FetchController,
                   ingestController: IngestController,
                   switchoverController: SwitchoverController) extends BaseController {

  def doGoAuto(target: String): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))

      val settings = WriterSettings(1, 0)
      val status = new StatusLogger(logger)
      workerFactory.worker.push(s"automatic search", status, {
        continuer =>
          val tree = sardine.exploreRemoteTree
          for (product <- KnownProducts.OSGB) {
            val found = tree.findLatestFor(product)
            if (found.isDefined) {
              val model = StateModel(found.get)
              pipeline(target, model, status, settings)
            }
          }
      })
      Accepted
  }

  def doGo(target: String, product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(1, 0)
      val model = new StateModel(product, epoch, variant, None)
      val status = new StatusLogger(logger)
      pipeline(target, model, status, settings)
      Accepted
  }

  private def pipeline(target: String, model1: StateModel, status: StatusLogger, settings: WriterSettings) {
    workerFactory.worker.push(s"finding ${model1.pathSegment}", status, {
      continuer =>
        if (continuer.isBusy) {
          val model2 = fetchController.fetch(model1, status)
          val model3 = ingestController.ingestIfOK(model2, status, settings, target, continuer)
          if (target == "db") {
            switchoverController.switchIfOK(model3, status)
          }
        }
    })
  }

}
