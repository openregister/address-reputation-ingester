/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package controllers

import java.io.File
import java.net.URL

import config.ApplicationGlobal
import config.ConfigHelper._
import fetch._
import play.api.Logger
import play.api.Play._
import services.es.IndexMetadata
import services.exec.{WorkQueue, WorkerFactory}
import uk.gov.hmrc.address.services.es.ElasticsearchHelper
import uk.gov.hmrc.logging.LoggerFacade

object ControllerConfig {

  // be careful to have only one thread pool in use
  implicit val ec = ApplicationGlobal.ec

  val realmString = mustGetConfigString(current.mode, current.configuration, "basicAuthentication.realm")

  private val appRemoteServer = mustGetConfigString(current.mode, current.configuration, "app.remote.server")
  Logger.info("app.remote.server=" + appRemoteServer)

  val proxyAuthInfo: Option[SardineAuthInfo] = getConfigString(current.mode, current.configuration, "proxy.host") match {
    case Some(proxyHost) =>
      val proxyPort = mustGetConfigString(current.mode, current.configuration, "proxy.port")
      val proxyProtocol = mustGetConfigString(current.mode, current.configuration, "proxy.protocol")
      val proxyUser = mustGetConfigString(current.mode, current.configuration, "proxy.username")
      val proxyPass = mustGetConfigString(current.mode, current.configuration, "proxy.password")

      System.getProperties.put(s"$proxyProtocol.proxyHost", proxyHost)
      System.getProperties.put(s"$proxyProtocol.proxyPort", proxyPort)

      Some(SardineAuthInfo(proxyHost, proxyPort.toInt, proxyUser, proxyPass))
    case None => None
  }

  val remoteServer = new URL(appRemoteServer)

  val remoteUser = mustGetConfigString(current.mode, current.configuration, "app.remote.user")
  val remotePass = mustGetConfigString(current.mode, current.configuration, "app.remote.pass")

  val downloadFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.downloadFolder")))
  val outputFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.outputFolder")))

  val workerFactory = new WorkerFactory()

  lazy val elasticSearchService: IndexMetadata = {
    val elasticSearchLocalMode = getConfigString(current.mode, current.configuration, "elastic.localmode").exists(_.toBoolean)
    if (elasticSearchLocalMode) {
      val client = ElasticsearchHelper.buildNodeLocalClient()
      new IndexMetadata(List(client), false, Map(), WorkQueue.statusLogger, ec)
    }
    else {
      val clusterName = mustGetConfigString(current.mode, current.configuration, "elastic.clustername")
      val connectionString = mustGetConfigString(current.mode, current.configuration, "elastic.uri")
      val isCluster = getConfigString(current.mode, current.configuration, "elastic.is-cluster").exists(_.toBoolean)
      val numShards = current.configuration.getConfig("elastic.shards").map(
        _.entrySet.foldLeft(Map.empty[String, Int])((m, a) => m + (a._1 -> a._2.unwrapped().asInstanceOf[Int]))
      ).getOrElse(Map.empty[String, Int])

      val clients = ElasticsearchHelper.buildNetClients(clusterName, connectionString, new LoggerFacade(Logger.logger))
      new IndexMetadata(clients, isCluster, numShards, WorkQueue.statusLogger, ec)
    }
  }

  val sardine = new SardineWrapper(remoteServer, remoteUser, remotePass, proxyAuthInfo, new SardineFactory2)

  val fetcher = new WebdavFetcher(sardine, downloadFolder, WorkQueue.statusLogger)

  val authAction = {
    val basicAuthFilterConfig = BasicAuthenticationFilterConfiguration.parse(current.mode, current.configuration)
    new BasicAuthenticatedAction(basicAuthFilterConfig)
  }
}
