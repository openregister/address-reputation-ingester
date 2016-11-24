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

import com.sksamuel.elastic4s.ElasticDsl
import controllers.SimpleValidator._
import play.api.mvc.{Action, ActionBuilder, AnyContent, Request}
import services.audit.AuditClient
import services.exec.{WorkQueue, WorkerFactory}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.es.{IndexMetadata, IndexName}
import uk.gov.hmrc.play.microservice.controller.BaseController

import scala.concurrent.ExecutionContext


object ElasticSwitchoverController extends SwitchoverController(
  ControllerConfig.authAction,
  WorkQueue.statusLogger,
  ControllerConfig.workerFactory,
  ControllerConfig.elasticSearchService,
  services.audit.Services.auditClient,
  "es",
  scala.concurrent.ExecutionContext.Implicits.global
)


class SwitchoverController(action: ActionBuilder[Request],
                           status: StatusLogger,
                           workerFactory: WorkerFactory,
                           indexMetadata: IndexMetadata,
                           auditClient: AuditClient,
                           target: String,
                           ec: ExecutionContext) extends BaseController with ElasticDsl {

  // TODO should these args just be the collection name?
  def doSwitchTo(product: String, epoch: Int, timestamp: String): Action[AnyContent] = action {
    request =>
      require(isAlphaNumeric(product))
      require(isTimestamp(timestamp))

      val model = new StateModel(product, Some(epoch), None, Some(timestamp), target = target)
      workerFactory.worker.push(s"switching to ${model.indexName.toString}", continuer => switchIfOK(model))
      Accepted
  }

  private[controllers] def switchIfOK(model: StateModel): StateModel = {
    if (model.hasFailed) {
      status.info("Switchover was skipped.")
      model // unchanged

    } else if (model.timestamp.isEmpty) {
      status.warn(s"cannot switch to ${model.indexName.toPrefix} with unknown date stamp")
      model.copy(hasFailed = true)

    } else {
      doSwitch(model)
    }
  }

  private def doSwitch(model: StateModel): StateModel = {

    val newName = model.indexName
    if (!indexMetadata.indexExists(newName)) {
      status.warn(s"$newName: collection was not found")
      model.copy(hasFailed = true)
    }
    else if (indexMetadata.findMetadata(newName).exists(_.completedAt.isDefined)) {
      // this metadata key/value is checked by all address-lookup nodes once every few minutes
      setIndexName(newName)
      status.info(s"Switched over to $newName")
      model // unchanged
    }
    else {
      status.warn(s"$newName: collection is still being written")
      model.copy(hasFailed = true)
    }
  }

  private def setIndexName(name: IndexName) {
    indexMetadata.setIndexInUse(name)
    auditClient.succeeded(Map("product" -> name.productName, "epoch" -> name.epoch.get.toString, "newCollection" -> name.toString))
  }
}
