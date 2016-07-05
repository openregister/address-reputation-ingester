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
 *
 */

package ingest

import java.net.URL

import config.ApplicationGlobal
import controllers.ControllerConfig
import controllers.SimpleValidator._
import fetch._
import play.api.mvc.{Action, AnyContent}
import services.db.{CollectionMetadata, OutputDBWriterFactory}
import services.exec.{Continuer, WorkerFactory}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object TranslateController extends TranslateController(
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ControllerConfig.fetcher,
  new IngesterFactory,
  new OutputDBWriterFactory,
  ControllerConfig.sardine,
  ControllerConfig.remoteServer,
  ApplicationGlobal.collectionMetadata)


class TranslateController(logger: StatusLogger,
                          workerFactory: WorkerFactory,
                          webdavFetcher: WebdavFetcher,
                          ingesterFactory: IngesterFactory,
                          dbWriterFactory: OutputDBWriterFactory,
                          sardine: SardineWrapper,
                          url: URL,
                          collectionMetadata: CollectionMetadata) extends BaseController {

  def doTranslate(product: String, epoch: Int, variant: String,
                  bulkSize: Option[Int], loopDelay: Option[Int],
                  forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = FetchControllerHelper.settings(bulkSize, loopDelay)
      val model = new StateModel(product, epoch, Some(variant), forceChange = forceChange getOrElse false)
      workerFactory.worker.push(s"fetching ${model.pathSegment}", continuer => translate(model, settings, continuer))
      Accepted("ok")
  }

  def translate(model1: StateModel, settings: WriterSettings, continuer: Continuer): StateModel = {
    val model2 =
      if (model1.product.isDefined) model1
      else {
        val tree = sardine.exploreRemoteTree
        val found = tree.findAvailableFor(model1.productName, model1.epoch.toString)
        model1.copy(product = found)
      }

    if (model2.product.isDefined) {
      val ingester = ingesterFactory.ingester(logger, continuer, dbWriterFactory, settings, model2.productName)
      ingester.begin()
      webdavFetcher.fetchList(model2.product.get, model2.pathSegment, model2.forceChange, continuer, ingester)
      model2
    }
    else
      model2.copy(hasFailed = true)
  }
}
