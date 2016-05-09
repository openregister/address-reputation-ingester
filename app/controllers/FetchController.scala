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

package controllers

import java.net.URL

import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.fetch.{WebdavFetcher, ZipUnpacker}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.logging.SimpleLogger
import uk.gov.hmrc.play.microservice.controller.BaseController
import controllers.SimpleValidator._


object FetchController extends FetchController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.fetcher,
  ControllerConfig.unzipper,
  ControllerConfig.remoteServer)


class FetchController(logger: SimpleLogger,
                      workerFactory: WorkerFactory,
                      webdavFetcher: WebdavFetcher,
                      unzipper: ZipUnpacker,
                      url: URL) extends BaseController {

  def doFetch(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val model = new StateModel(product, epoch, variant, None)
      val status = new StatusLogger(logger)
      val worker = workerFactory.worker

      worker.push(s"fetching ${model.pathSegment}", status, {
        continuer =>
          fetch(model, status)
      })
      Accepted("ok")
  }

  private[controllers] def fetch(model: StateModel, status: StatusLogger): StateModel = {
    val files = webdavFetcher.fetchAll(s"$url/${model.pathSegment}", model.pathSegment)
    unzipper.unzipList(files, model.pathSegment)
    model
  }
}
