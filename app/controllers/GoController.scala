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

import java.net.URL

import controllers.SimpleValidator._
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.fetch.SardineWrapper
import services.model.{StateModel, StatusLogger}
import services.writers.{OutputDBWriterFactory, WriterSettings}
import uk.co.hmrc.logging.SimpleLogger
import uk.gov.hmrc.play.microservice.controller.BaseController

object GoController extends GoController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.sardine,
  ControllerConfig.remoteServer,
  ControllerConfig.remoteUser,
  ControllerConfig.remotePass,
  new OutputDBWriterFactory
)

class GoController(logger: SimpleLogger,
                   workerFactory: WorkerFactory,
                   sardine: SardineWrapper,
                   url: URL,
                   username: String,
                   password: String,
                   dbWriterFactory: OutputDBWriterFactory) extends BaseController {

  def goAuto: Action[AnyContent] = Action {
    request =>
      val settings = WriterSettings(1, 0)
      val model1 = new StateModel()
      val status = new StatusLogger(logger)
      workerFactory.worker.push(s"finding ${model1.pathSegment}", status, {
        continuer =>
          val tree = sardine.exploreRemoteTree(url, username, password)
          val files = tree.findLatestFor(model1.product)
          pipeline(model1, status, settings)
      })
      Accepted
  }

  def doGo(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(1, 0)
      val model1 = new StateModel(product, epoch, variant, None)
      val status = new StatusLogger(logger)
      pipeline(model1, status, settings)
      Accepted
  }

  private def pipeline(model1: StateModel, status: StatusLogger, settings: WriterSettings) {
    workerFactory.worker.push(s"finding ${model1.pathSegment}", status, {
      continuer =>
        val model2 = FetchController.fetch(model1, status)
        val model3 = IngestController.ingestIfOK(model2, status, settings, dbWriterFactory, continuer)
        SwitchoverController.switchIfOK(model3, status)
    })
  }

}
