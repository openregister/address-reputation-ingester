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
import services.es.{ElasticsearchHelper, IndexMetadata}
import services.mongo.{CollectionMetadata, MongoSystemMetadataStoreFactory}
import services.exec.WorkQueue
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.LoggerFacade
import uk.gov.hmrc.play.config.RunMode
import uk.gov.hmrc.play.graphite.GraphiteConfig
import uk.gov.hmrc.play.microservice.bootstrap.JsonErrorHandling
import uk.gov.hmrc.play.microservice.bootstrap.Routing.RemovingOfTrailingSlashes

object ApplicationGlobal extends GlobalSettings with GraphiteConfig with RemovingOfTrailingSlashes with JsonErrorHandling with RunMode {

  lazy val mongoConnection = {
    val mongoDbUri = mustGetConfigString(current.mode, current.configuration, "mongodb.uri")
    new CasbahMongoConnection(mongoDbUri)
  }

  lazy val metadataStore = new MongoSystemMetadataStoreFactory().newStore(mongoConnection)

  lazy val mongoCollectionMetadata = new CollectionMetadata(mongoConnection.getConfiguredDb, metadataStore)

  lazy val elasticSearchService: IndexMetadata = {
    if (elasticSearchLocalMode) {
      ElasticsearchHelper(scala.concurrent.ExecutionContext.Implicits.global)
    }
    else {
      val clusterName = mustGetConfigString(current.mode, current.configuration, "elastic.clustername")
      val connectionString = mustGetConfigString(current.mode, current.configuration, "elastic.uri")
      val isCluster = getConfigBoolean(current.mode, current.configuration, "elastic.is-cluster").getOrElse(true)
      val numShards = getConfigInt(current.mode, current.configuration, "elastic.num-shards").getOrElse(12)

      ElasticsearchHelper(clusterName, connectionString, isCluster, numShards,
      scala.concurrent.ExecutionContext.Implicits.global, new LoggerFacade(Logger.logger))
    }
  }

  lazy val elasticSearchLocalMode: Boolean = {
    getConfigBoolean(current.mode, current.configuration, "elastic.localmode").getOrElse(false)
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

