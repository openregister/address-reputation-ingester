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

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.MutateAliasDefinition
import config.ApplicationGlobal
import controllers.SimpleValidator._
import org.elasticsearch.cluster.health.ClusterHealthStatus
import play.api.Logger
import play.api.mvc.{Action, ActionBuilder, AnyContent, Request}
import services.audit.AuditClient
import services.db.CollectionMetadata
import services.es.{ElasticsearchHelper, Services}
import services.exec.WorkerFactory
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.admin.{MetadataStore, StoredMetadataItem}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Try

object SwitchoverController extends SwitchoverController(
  ControllerConfig.authAction,
  ControllerConfig.logger,
  ControllerConfig.workerFactory,
  ApplicationGlobal.mongoConnection,
  ApplicationGlobal.metadataStore,
  services.audit.Services.auditClient,
  Services.elasticSearchService
)

class SwitchoverController(action: ActionBuilder[Request],
                           status: StatusLogger,
                           workerFactory: WorkerFactory,
                           mongoDbConnection: CasbahMongoConnection,
                           systemMetadata: MongoSystemMetadataStore,
                           auditClient: AuditClient,
                           esHelper: ElasticsearchHelper) extends BaseController {

  def isSupportedTarget(target: String): Boolean = Set("db", "es").contains(target)

  def doSwitchTo(target: String, product: String, epoch: Int, modifier: String): Action[AnyContent] = action {
    request =>
      require(isAlphaNumeric(product))
      require(isSupportedTarget(target))

      val version = Try(modifier.toInt).toOption
      val dateStamp = if (version.isEmpty) Some(modifier) else None

      val model = new StateModel(product, epoch, None, version, dateStamp, target = target)
      workerFactory.worker.push(s"switching to ${model.collectionName.toString}", continuer => switchIfOK(model))
      Accepted
  }

  private[controllers] def switchIfOK(model: StateModel): StateModel = {
    if (!model.hasFailed) {
      if (model.target == "es") {
        switchEs(model)
      }
      else
        switchDb(model)
    } else {
      status.info("Switchover was skipped.")
      model // unchanged
    }
  }

  private def switchDb(model: StateModel): StateModel = {

    if (model.version.isEmpty) {
      status.warn(s"cannot switch to ${model.collectionName.toPrefix} with unknown index")
      model.copy(hasFailed = true)

    } else {
      val newName = model.collectionName

      val db = mongoDbConnection.getConfiguredDb
      val collectionMetadata = new CollectionMetadata(db)
      if (!db.collectionExists(newName.toString)) {
        status.warn(s"$newName: collection was not found")
        model.copy(hasFailed = true)
      }
      else if (collectionMetadata.findMetadata(newName).exists(_.completedAt.isDefined)) {
        // this metadata key/value is checked by all address-lookup nodes once every few minutes
        setCollectionName(model.productName, model.epoch, newName.toString)
        status.info(s"Switched over to $newName")
        model // unchanged
      }
      else {
        status.warn(s"$newName: collection is still being written")
        model.copy(hasFailed = true)
      }
    }
  }

  private def switchEs(model: StateModel): StateModel = {

    val ariIndexName = model.collectionName.asIndexName
    val ariAliasName = esHelper.ariAliasName

    import scala.concurrent.ExecutionContext.Implicits.global

    val fr = esHelper.clients map { client => Future {
      if (esHelper.isCluster) {
        status.info(s"Setting replica count to ${esHelper.replicaCount} for $ariAliasName")
        client execute {
          update settings ariIndexName set Map(
            "index.number_of_replicas" -> esHelper.replicaCount
          )
        } await

        status.info(s"Waiting for $ariAliasName to go green after increasing replica count")

        blockUntil("Expected cluster to have green status", 1200) { () =>
          client.execute {
            get cluster health
          }.await.getStatus == ClusterHealthStatus.GREEN
        }
      }

      val gar = client execute {
        getAlias(model.productName).on("*")
      } await

      val olc = gar.getAliases.keys
      val aliasStatements: Array[MutateAliasDefinition] = olc.toArray().flatMap(a => {
        val aliasIndex = a.asInstanceOf[String]
        status.info(s"Removing index $aliasIndex from $ariAliasName and ${model.productName} aliases")
        Array(remove alias ariAliasName on aliasIndex, remove alias model.productName on aliasIndex)
      })

      val resp = client execute {
        aliases(
          aliasStatements ++
            Seq(
              add alias ariAliasName on ariIndexName,
              add alias model.productName on ariIndexName
            )
        )
      }
      status.info(s"Adding index $ariIndexName to $ariAliasName and ${model.productName}")

      olc.toArray().foreach(a => {
        val aliasIndex = a.asInstanceOf[String]
        status.info(s"Reducing replica count for $aliasIndex to 0")
        val replicaResp = client execute {
          update settings aliasIndex set Map(
            "index.number_of_replicas" -> "0"
          )
        } await
      })
    }
    }

    Await.result(Future.sequence(fr), Duration.Inf)
    model
  }

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

  private def setCollectionName(productName: String, epoch: Int, newName: String) {
    val addressBaseCollectionName = systemMetadata.addressBaseCollectionItem(productName)
    addressBaseCollectionName.set(newName)
    auditClient.succeeded(Map("product" -> productName, "epoch" -> epoch.toString, "newCollection" -> newName))
  }
}


class MongoSystemMetadataStoreFactory {
  def newStore(mongo: CasbahMongoConnection): MongoSystemMetadataStore =
    new MongoSystemMetadataStore(mongo, new LoggerFacade(Logger.logger))
}


class MongoSystemMetadataStore(mongo: CasbahMongoConnection, logger: SimpleLogger) {
  private val store = new MetadataStore(mongo, logger)
  private val table = Map(
    "abp" -> store.gbAddressBaseCollectionName,
    "abi" -> store.niAddressBaseCollectionName
  )

  def addressBaseCollectionItem(productName: String): StoredMetadataItem = {
    val cn = table.get(productName)
    if (cn.isEmpty) {
      throw new IllegalArgumentException(s"Unsupported product $productName")
    }
    cn.get
  }
}
