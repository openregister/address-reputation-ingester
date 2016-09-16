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

import config.ApplicationGlobal
import controllers.SimpleValidator._
import play.api.mvc.{Action, ActionBuilder, AnyContent, Request}
import services.DbFacade
import services.audit.AuditClient
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import services.mongo.CollectionName
import uk.gov.hmrc.play.microservice.controller.BaseController

import scala.concurrent.ExecutionContext


object MongoSwitchoverController extends SwitchoverController(
  ControllerConfig.authAction,
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ApplicationGlobal.mongoCollectionMetadata,
  services.audit.Services.auditClient,
  "db",
  scala.concurrent.ExecutionContext.Implicits.global
)


object ElasticSwitchoverController extends SwitchoverController(
  ControllerConfig.authAction,
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ApplicationGlobal.elasticSearchService,
  services.audit.Services.auditClient,
  "es",
  scala.concurrent.ExecutionContext.Implicits.global
)


class SwitchoverController(action: ActionBuilder[Request],
                           status: StatusLogger,
                           workerFactory: WorkerFactory,
                           collectionMetadata: DbFacade,
                           auditClient: AuditClient,
                           target: String,
                           ec: ExecutionContext) extends BaseController {

  // TODO should these args just be the collection name?
  def doSwitchTo(product: String, epoch: Int, timestamp: String): Action[AnyContent] = action {
    request =>
      require(isAlphaNumeric(product))
      require(isTimestamp(timestamp))

      val model = new StateModel(product, epoch, None, Some(timestamp), target = target)
      workerFactory.worker.push(s"switching to ${model.collectionName.toString}", continuer => switchIfOK(model))
      Accepted
  }

  private[controllers] def switchIfOK(model: StateModel): StateModel = {
    if (model.hasFailed) {
      status.info("Switchover was skipped.")
      model // unchanged

    } else if (model.timestamp.isEmpty) {
      status.warn(s"cannot switch to ${model.collectionName.toPrefix} with unknown date stamp")
      model.copy(hasFailed = true)

    } else {
      switchDb(model)
    }
  }

  private def switchDb(model: StateModel): StateModel = {

    val newName = model.collectionName
    if (!collectionMetadata.collectionExists(newName.toString)) {
      status.warn(s"$newName: collection was not found")
      model.copy(hasFailed = true)
    }
    else if (collectionMetadata.findMetadata(newName).exists(_.completedAt.isDefined)) {
      // this metadata key/value is checked by all address-lookup nodes once every few minutes
      setCollectionName(newName)
      status.info(s"Switched over to $newName")
      model // unchanged
    }
    else {
      status.warn(s"$newName: collection is still being written")
      model.copy(hasFailed = true)
    }
  }

//  private def switchEs(model: StateModel): StateModel = {
  //
  //    implicit val ecx = ec
  //    val ariIndexName = model.collectionName.toString
  //    val ariAliasName = indexMetadata.ariAliasName
  //
  //    val fr = indexMetadata.clients map {
  //      client => Future {
  //        if (indexMetadata.isCluster) {
  //          status.info(s"Setting replica count to ${
  //            indexMetadata.replicaCount
  //          } for $ariAliasName")
  //          client execute {
  //            update settings ariIndexName set Map(
  //              "index.number_of_replicas" -> indexMetadata.replicaCount
  //            )
  //          } await
  //
  //          status.info(s"Waiting for $ariAliasName to go green after increasing replica count")
  //
  //          blockUntil("Expected cluster to have green status", 1200) {
  //            () =>
  //              client.execute {
  //                get cluster health
  //              }.await.getStatus == ClusterHealthStatus.GREEN
  //          }
  //        }
  //
  //        val gar = client execute {
  //          getAlias(model.productName).on("*")
  //        } await
  //
  //        val olc = gar.getAliases.keys
  //        val aliasStatements: Array[MutateAliasDefinition] = olc.toArray().flatMap(a => {
  //          val aliasIndex = a.asInstanceOf[String]
  //          status.info(s"Removing index $aliasIndex from $ariAliasName and ${
  //            model.productName
  //          } aliases")
  //          Array(remove alias ariAliasName on aliasIndex, remove alias model.productName on aliasIndex)
  //        })
  //
  //        val resp = client execute {
  //          aliases(
  //            aliasStatements ++
  //              Seq(
  //                add alias ariAliasName on ariIndexName,
  //                add alias model.productName on ariIndexName
  //              )
  //          )
  //        }
  //        status.info(s"Adding index $ariIndexName to $ariAliasName and ${
  //          model.productName
  //        }")
  //
  //        olc.toArray().foreach(a => {
  //          val aliasIndex = a.asInstanceOf[String]
  //          status.info(s"Reducing replica count for $aliasIndex to 0")
  //          val replicaResp = client execute {
  //            update settings aliasIndex set Map(
  //              "index.number_of_replicas" -> "0"
  //            )
  //          } await
  //        })
  //      }
  //    }
  //
  //    Await.result(Future.sequence(fr), Duration.Inf)
  //    model
  //  }

  private def blockUntil(explain: String, waitFor: Int = 10)(predicate: () => Boolean): Unit = {

    var count = 0
    var done = false

    while (count < waitFor && !done) {
      Thread.sleep(1000)
      count = count + 1
      done = predicate()
    }

    require(done, s"Failed waiting on: $explain")
  }

  private def setCollectionName(name: CollectionName) {
    collectionMetadata.setCollectionInUseFor(name)
    auditClient.succeeded(Map("product" -> name.productName, "epoch" -> name.epoch.get.toString, "newCollection" -> name.toString))
  }
}
