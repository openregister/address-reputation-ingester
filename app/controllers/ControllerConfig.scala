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
import services.exec.{WorkQueue, WorkerFactory}
import uk.gov.hmrc.address.services.es._
import uk.gov.hmrc.logging.LoggerFacade

object ControllerConfig {

  // be careful to have only one thread pool in use
  implicit val ec = ApplicationGlobal.ec

  val environmentString = mustGetConfigString(current.mode, current.configuration, "app.environment")

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
    val localMode = getConfigString(current.mode, current.configuration, "elastic.localMode").exists(_.toBoolean)
    val homeDir = getConfigString(current.mode, current.configuration, "elastic.homeDir")
    val preDelete = getConfigString(current.mode, current.configuration, "elastic.preDelete").exists(_.toBoolean)
    val clusterName = mustGetConfigString(current.mode, current.configuration, "elastic.clusterName")
    val connectionString = mustGetConfigString(current.mode, current.configuration, "elastic.uri")
    val isCluster = getConfigString(current.mode, current.configuration, "elastic.isCluster").exists(_.toBoolean)
    val numShards = current.configuration.getConfig("elastic.shards").map(
      _.entrySet.foldLeft(Map.empty[String, Int])((m, a) => m + (a._1 -> a._2.unwrapped().asInstanceOf[Int]))
    ).getOrElse(Map.empty[String, Int])

    val settings = ElasticSettings(localMode, homeDir, preDelete, connectionString, isCluster, clusterName, numShards)
    val clients = ElasticsearchHelper.buildClients(settings, new LoggerFacade(Logger.logger))
    val esImpl = new ESAdminImpl(clients, WorkQueue.statusLogger, ec)
    new IndexMetadata(esImpl, isCluster, numShards, WorkQueue.statusLogger, ec)
  }

  val sardine = new SardineWrapper(remoteServer, remoteUser, remotePass, proxyAuthInfo, new SardineFactory2)

  val fetcher = new WebdavFetcher(sardine, downloadFolder, WorkQueue.statusLogger)
}
