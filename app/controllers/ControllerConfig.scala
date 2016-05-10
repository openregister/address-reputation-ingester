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
import play.api.Logger
import play.api.Play._
import services.exec.WorkerFactory
import services.fetch._
import uk.co.hmrc.logging.LoggerFacade

object ControllerConfig {

  val logger = new LoggerFacade(Logger.logger)

  val remoteServer = new URL(mustGetConfigString(current.mode, current.configuration, "app.remote.server"))
  val remoteUser = mustGetConfigString(current.mode, current.configuration, "app.remote.user")
  val remotePass = mustGetConfigString(current.mode, current.configuration, "app.remote.pass")
  val downloadFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.downloadFolder")))
  val unpackFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.unpackFolder")))

  val workerFactory = new WorkerFactory()
  val sardine = new SardineWrapper(remoteServer, remoteUser, remotePass, logger, new SardineFactory2)
  val fetcher = new WebdavFetcher(logger, sardine, downloadFolder)
  val unzipper = new ZipUnpacker(logger, unpackFolder)
}