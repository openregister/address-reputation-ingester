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

import config.ConfigHelper._
import fetch.{SardineFactory2, SardineWrapper, WebdavFetcher, ZipUnpacker}
import play.api.Logger
import play.api.Play._
import services.exec.WorkerFactory

object ControllerConfig {

  private val appRemoteServer = mustGetConfigString(current.mode, current.configuration, "app.remote.server")
  Logger.info("app.remote.server=" + appRemoteServer)

  val remoteServer = new URL(appRemoteServer)

  val remoteUser = mustGetConfigString(current.mode, current.configuration, "app.remote.user")
  val remotePass = mustGetConfigString(current.mode, current.configuration, "app.remote.pass")

  val downloadFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.downloadFolder")))
  val outputFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.outputFolder")))

  val workerFactory = new WorkerFactory()
  val logger = workerFactory.worker.statusLogger

  val sardine = new SardineWrapper(remoteServer, remoteUser, remotePass, new SardineFactory2)
  val fetcher = new WebdavFetcher(sardine, downloadFolder, logger)
  val unzipper = new ZipUnpacker(downloadFolder, logger)

  val authAction = {
    val basicAuthFilterConfig = BasicAuthenticationFilterConfiguration.parse(current.mode, current.configuration)
    new BasicAuthenticatedAction(basicAuthFilterConfig)
  }
}
