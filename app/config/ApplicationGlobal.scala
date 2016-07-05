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
import controllers.SystemMetadataStoreFactory
import play.api.Play._
import play.api._
import services.db.CollectionMetadata
import services.exec.WorkQueue
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.gov.hmrc.play.config.RunMode
import uk.gov.hmrc.play.graphite.GraphiteConfig
import uk.gov.hmrc.play.microservice.bootstrap.JsonErrorHandling
import uk.gov.hmrc.play.microservice.bootstrap.Routing.RemovingOfTrailingSlashes

object ApplicationGlobal extends GlobalSettings with GraphiteConfig with RemovingOfTrailingSlashes with JsonErrorHandling with RunMode {

  lazy val mongoConnection = {
    val mongoDbUri = mustGetConfigString(current.mode, current.configuration, "mongodb.uri")
    new CasbahMongoConnection(mongoDbUri)
  }

  lazy val metadataStore = new SystemMetadataStoreFactory().newStore(mongoConnection)

  lazy val collectionMetadata = new CollectionMetadata(mongoConnection.getConfiguredDb)

  override def onStart(app: Application) {
    println("########## onStart ##########")
    val config = app.configuration
    val appName = config.getString("appName").getOrElse("APP NAME NOT SET")
    Logger.info(s"Starting microservice : $appName : in mode : ${app.mode}")
    Logger.info(Provenance.versionInfo)
//    Logger.info(s"address-reputation-ingestor config:\n${config.underlying.toString}\n")
    super.onStart(app)
  }

  override def microserviceMetricsConfig(implicit app: Application): Option[Configuration] = app.configuration.getConfig(s"$env.metrics")

  override def onStop(app: Application): Unit = {
    println("########## onStop ##########")
    WorkQueue.singleton.terminate()
    mongoConnection.close()
  }
}

