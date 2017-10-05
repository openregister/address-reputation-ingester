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

import java.net.URL
import javax.inject.Singleton

import com.google.inject.{AbstractModule, Provides}
import config.ConfigHelper
import controllers.ControllerConfig
import fetch.{SardineAuthInfo, SardineFactory2, SardineWrapper, WebdavFetcher}
import play.api.{Configuration, Environment, Logger}
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es.{ESAdminImpl, ElasticSettings, ElasticsearchHelper, IndexMetadata}
import uk.gov.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.concurrent.ExecutionContext

class Module(environment: Environment,
             configuration: Configuration) extends AbstractModule {

  lazy val numShards = configuration.getConfig("elastic.shards").map(
    _.entrySet.foldLeft(Map.empty[String, Int])((m, a) => m + (a._1 -> a._2.unwrapped().asInstanceOf[Int]))
  ).getOrElse(Map.empty[String, Int])

  def configure(): Unit = {}

  @Provides
  @Singleton
  def provideLogger: SimpleLogger = new LoggerFacade(play.api.Logger.logger)

  @Provides
  @Singleton
  def provideStatusLogger: StatusLogger = new StatusLogger(new LoggerFacade(Logger.logger))

  @Provides
  @Singleton
  def provideElasticSettings(configHelper: ConfigHelper): ElasticSettings = {
    val localMode = configHelper.getConfigString("elastic.localMode").exists(_.toBoolean)
    val homeDir = configHelper.getConfigString("elastic.homeDir")
    val preDelete = configHelper.getConfigString("elastic.preDelete").exists(_.toBoolean)
    val clusterName = configHelper.mustGetConfigString("elastic.clusterName")
    val connectionString = configHelper.mustGetConfigString("elastic.uri")
    val isCluster = configHelper.getConfigString("elastic.isCluster").exists(_.toBoolean)
    ElasticSettings(localMode, homeDir, preDelete, connectionString, isCluster, clusterName, numShards)
  }

  @Provides
  @Singleton
  def provideIndexMetadata(configHelper: ConfigHelper, statusLogger: StatusLogger, ec: ExecutionContext, settings: ElasticSettings): IndexMetadata = {
    val clients = ElasticsearchHelper.buildClients(settings, new LoggerFacade(Logger.logger))
    val esImpl = new ESAdminImpl(clients, statusLogger, ec, settings)
    new IndexMetadata(esImpl, settings.isCluster, numShards, statusLogger, ec)
  }

  @Provides
  @Singleton
  def provideSardine(configHelper: ConfigHelper): SardineWrapper = {
    val proxyAuthInfo: Option[SardineAuthInfo] = configHelper.getConfigString("proxy.host") match {
      case Some(proxyHost) =>
        val proxyPort = configHelper.mustGetConfigString("proxy.port")
        val proxyProtocol = configHelper.mustGetConfigString("proxy.protocol")
        val proxyUser = configHelper.mustGetConfigString("proxy.username")
        val proxyPass = configHelper.mustGetConfigString("proxy.password")

        System.getProperties.put(s"$proxyProtocol.proxyHost", proxyHost)
        System.getProperties.put(s"$proxyProtocol.proxyPort", proxyPort)

        Some(SardineAuthInfo(proxyHost, proxyPort.toInt, proxyUser, proxyPass))
      case None => None
    }

    val appRemoteServer = configHelper.mustGetConfigString("app.remote.server")
    Logger.info("app.remote.server=" + appRemoteServer)

    val remoteServer = new URL(appRemoteServer)

    val remoteUser = configHelper.mustGetConfigString("app.remote.user")
    val remotePass = configHelper.mustGetConfigString("app.remote.pass")

    new SardineWrapper(remoteServer, remoteUser, remotePass, proxyAuthInfo, new SardineFactory2)
  }

  @Provides
  @Singleton
  def providesFetcher(cc: ControllerConfig, configHelper: ConfigHelper, sardine: SardineWrapper, statusLogger: StatusLogger) = {
    new WebdavFetcher(sardine, cc.downloadFolder, statusLogger)
  }
}