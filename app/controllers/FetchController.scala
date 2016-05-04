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

import java.io.File

import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.WorkerFactory
import services.ingester.fetch.{SardineFactory2, WebdavFetcher, ZipUnpacker}
import services.ingester.model.StateModel
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object FetchControllerConfig {
  val logger = new LoggerFacade(Logger.logger)

  val remoteServer = mustGetConfigString(current.mode, current.configuration, "app.remote.server")
  val remoteUser = mustGetConfigString(current.mode, current.configuration, "app.remote.user")
  val remotePass = mustGetConfigString(current.mode, current.configuration, "app.remote.pass")
  val downloadFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.downloadFolder")))
  val unpackFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.unpackFolder")))

  val fetcher = new WebdavFetcher(FetchControllerConfig.logger, new SardineFactory2, downloadFolder)
  val unzipper = new ZipUnpacker(FetchControllerConfig.logger, unpackFolder)
}


object FetchController extends FetchController(
  new WorkerFactory(),
  FetchControllerConfig.logger,
  FetchControllerConfig.fetcher,
  FetchControllerConfig.unzipper,
  FetchControllerConfig.remoteServer,
  FetchControllerConfig.remoteUser,
  FetchControllerConfig.remotePass
)


class FetchController(workerFactory: WorkerFactory,
                      logger: SimpleLogger,
                      webdavFetcher: WebdavFetcher,
                      unzipper: ZipUnpacker,
                      url: String,
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
        webdavFetcher.fetchAll(s"$url/${model.pathSegment}", username, password, model.pathSegment)
//        unzipper.unzip()
    })
  }
}
