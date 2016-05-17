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

import java.net.URL

import controllers.ControllerConfig
import controllers.SimpleValidator._
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object FetchController extends FetchController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.fetcher,
  ControllerConfig.unzipper,
  ControllerConfig.remoteServer)


class FetchController(logger: StatusLogger,
                      workerFactory: WorkerFactory,
                      webdavFetcher: WebdavFetcher,
                      unzipper: ZipUnpacker,
                      url: URL) extends BaseController {

  def doFetch(product: String, epoch: Int, variant: String, forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val model = new StateModel(product, epoch, Some(variant), forceChange = forceChange getOrElse false)
      workerFactory.worker.push(s"fetching ${model.pathSegment}", {
        continuer =>
          fetch(model)
      })
      Accepted("ok")
  }

  def fetch(model: StateModel): StateModel = {
    val files: List[DownloadItem] =
      if (model.product.nonEmpty) {
        webdavFetcher.fetchList(model.product.get, model.pathSegment, model.forceChange)
      } else {
        webdavFetcher.fetchAll(s"$url/${model.pathSegment}", model.pathSegment, model.forceChange)
      }

    val freshItems = files.filter(_.fresh)
    val toUnzip = freshItems.map(_.file)
    unzipper.unzipList(toUnzip, model.pathSegment)

    if (files.nonEmpty) model else model.copy(hasFailed = true)
  }
}
