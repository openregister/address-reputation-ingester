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

package config

import config.ConfigHelper._
import play.api.Play._
import play.api._
import services.es.IndexMetadata
import services.exec.WorkQueue
import services.mongo.{CollectionMetadata, MongoSystemMetadataStoreFactory}
import uk.gov.hmrc.address.services.es.ElasticsearchHelper
import uk.gov.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.logging.LoggerFacade
import uk.gov.hmrc.play.config.RunMode
import uk.gov.hmrc.play.graphite.GraphiteConfig
import uk.gov.hmrc.play.microservice.bootstrap.JsonErrorHandling
import uk.gov.hmrc.play.microservice.bootstrap.Routing.RemovingOfTrailingSlashes

object ApplicationGlobal extends GlobalSettings with GraphiteConfig with RemovingOfTrailingSlashes with JsonErrorHandling with RunMode {

  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  lazy val mongoConnection = {
    val mongoDbUri = mustGetConfigString(current.mode, current.configuration, "mongodb.uri")
    new CasbahMongoConnection(mongoDbUri)
  }

  lazy val metadataStore = new MongoSystemMetadataStoreFactory().newStore(mongoConnection)

  lazy val mongoCollectionMetadata = new CollectionMetadata(mongoConnection.getConfiguredDb, metadataStore)

  lazy val elasticSearchService: IndexMetadata = {
    val elasticSearchLocalMode = getConfigString(current.mode, current.configuration, "elastic.localmode").exists(_.toBoolean)
    if (elasticSearchLocalMode) {
      val client = ElasticsearchHelper.buildNodeLocalClient()
      new IndexMetadata(List(client), false, Map())
    }
    else {
      val clusterName = mustGetConfigString(current.mode, current.configuration, "elastic.clustername")
      val connectionString = mustGetConfigString(current.mode, current.configuration, "elastic.uri")
      val isCluster = getConfigString(current.mode, current.configuration, "elastic.is-cluster").exists(_.toBoolean)
      val numShards = current.configuration.getConfig("elastic.shards").map(
        _.entrySet.foldLeft(Map.empty[String, Int])((m, a) => m + (a._1 -> a._2.unwrapped().asInstanceOf[Int]))
      ).getOrElse(Map.empty[String, Int])

      val clients = ElasticsearchHelper.buildNetClients(clusterName, connectionString, new LoggerFacade(Logger.logger))
      new IndexMetadata(clients, isCluster, numShards)
    }
  }

  override def onStart(app: Application) {
    val config = app.configuration
    val appName = config.getString("appName").getOrElse("APP NAME NOT SET")
    Logger.info(s"Starting microservice : $appName : in mode : ${app.mode}")
    Logger.info(Provenance.versionInfo)
    super.onStart(app)
  }

  override def microserviceMetricsConfig(implicit app: Application): Option[Configuration] = app.configuration.getConfig(s"$env.metrics")

  override def onStop(app: Application): Unit = {
    Logger.info(s"Stopping microservice")
    WorkQueue.singleton.terminate()
    mongoConnection.close()
  }
}

