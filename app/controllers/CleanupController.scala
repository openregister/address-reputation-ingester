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

package controllers

import java.io.File

import ingest.IngestControllerHelper
import play.api.mvc.{Action, AnyContent}
import services.exec.WorkerFactory
import services.model.StatusLogger
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.play.microservice.controller.BaseController

class CleanupController(logger: StatusLogger,
                        workerFactory: WorkerFactory,
                        downloadFolder: File,
                        mongoDbConnection: CasbahMongoConnection,
                        systemMetadata: SystemMetadataStore) extends BaseController {

  def doCleanup(target: String): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))

      target match {
        case "file" =>
          workerFactory.worker.push("cleaning up obsolete files", {
            continuer =>
              deleteObsoleteFiles()
          })

        case "db" =>
          workerFactory.worker.push("cleaning up obsolete collections", {
            continuer =>
              deleteObsoleteCollections()
          })
      }
      Accepted
  }


  private def deleteObsoleteFiles() {}

  private def deleteObsoleteCollections() {}
}
