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
import services.model.StateModel
import uk.co.hmrc.logging.SimpleLogger
import uk.gov.hmrc.play.microservice.controller.BaseController


object FetchController extends FetchController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.fetcher,
  ControllerConfig.unzipper,
  ControllerConfig.remoteServer,
  ControllerConfig.remoteUser,
  ControllerConfig.remotePass)


class FetchController(logger: SimpleLogger,
                      workerFactory: WorkerFactory,
                      webdavFetcher: WebdavFetcher,
                      unzipper: ZipUnpacker,
                      url: URL,
                      username: String,
                      password: String) extends BaseController {

  def fetch(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      val model = new StateModel(logger, product, epoch, variant, None)
      val status = queueFetch(model)
      Accepted(status.toString)
  }

  private[controllers] def queueFetch(model: StateModel): Boolean = {
    val worker = workerFactory.worker

    worker.push(s"fetching ${model.pathSegment}", model, {
      continuer =>
        val files = webdavFetcher.fetchAll(s"$url/${model.pathSegment}", username, password, model.pathSegment)
        unzipper.unzipList(files, model.pathSegment)
    })
  }
}
