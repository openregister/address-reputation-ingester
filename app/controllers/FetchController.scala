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

import java.nio.file.{Path, Paths}

import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.WorkerFactory
import services.ingester.fetch.{SardineFactory2, WebdavFetcher}
import services.ingester.model.StateModel
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object FetchControllerConfig {
  val logger = new LoggerFacade(Logger.logger)

  val remoteServer = mustGetConfigString(current.mode, current.configuration, "app.remote.server")
  val remoteUser = mustGetConfigString(current.mode, current.configuration, "app.remote.user")
  val remotePass = mustGetConfigString(current.mode, current.configuration, "app.remote.pass")
  val rootFolder = mustGetConfigString(current.mode, current.configuration, "app.files.rootFolder")
}


object FetchController extends FetchController(
  new WorkerFactory(),
  FetchControllerConfig.logger,
  new WebdavFetcher(FetchControllerConfig.logger, new SardineFactory2),
  FetchControllerConfig.remoteServer,
  FetchControllerConfig.remoteUser,
  FetchControllerConfig.remotePass,
  Paths.get(FetchControllerConfig.rootFolder)
)


class FetchController(workerFactory: WorkerFactory,
                      logger: SimpleLogger,
                      webdavFetcher: WebdavFetcher,
                      url: String,
                      username: String,
                      password: String,
                      outputDirectory: Path) extends BaseController {

  def fetch(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      val model = new StateModel(product, epoch, variant, None, logger)
      val status = queueFetch(model)
      Accepted(status.toString)
  }

  private[controllers] def queueFetch(model: StateModel): Boolean = {
    val worker = workerFactory.worker

    worker.push(s"fetching ${model.pathSegment}", {
      continuer =>
        val dir = outputDirectory.resolve(model.pathSegment)
        webdavFetcher.fetchAll(s"$url/${model.pathSegment}", username, password, dir)
    })
  }
}
