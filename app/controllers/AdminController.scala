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

package controllers

import java.io.File
import java.net.URL

import fetch.WebDavFile
import play.api.mvc.{Action, AnyContent, Request, Result}
import services.exec.WorkQueue
import uk.gov.hmrc.play.microservice.controller.BaseController

object AdminController extends AdminController(WorkQueue.singleton)

class AdminController(worker: WorkQueue) extends BaseController {

  def cancelTask(): Action[AnyContent] = Action {
    request => {
      handleCancelTask(request)
    }
  }

  def handleCancelTask(request: Request[AnyContent]): Result = {
    if (worker.abort()) {
      Ok(worker.status)
    } else {
      BadRequest(worker.status)
    }
  }

  def status(): Action[AnyContent] = Action {
    request => {
      Ok(worker.status).withHeaders(CONTENT_TYPE -> "text/plain")
    }
  }

  def fullStatus(): Action[AnyContent] = Action {
    request => {
      Ok(worker.fullStatus).withHeaders(CONTENT_TYPE -> "text/plain")
    }
  }

  def dirTree(): Action[AnyContent] = Action {
    request => {
      val tree = listFiles(ControllerConfig.downloadFolder)
      Ok(tree.toString).withHeaders(CONTENT_TYPE -> "text/plain")
    }
  }

  private def listFiles(dir: File): WebDavFile = {
    val files = dir.listFiles.toList.sorted
    val (dirs, plains) = files.partition(_.isDirectory)
    val subs = dirs.map(listFiles)
    val others = plains.map(toWebDavFile)
    WebDavFile(new URL("file://" + dir.getPath), dir.getName, 0L, true, false, false, subs ++ others)
  }

  private def toWebDavFile(file: File): WebDavFile = {
    val name = file.getName.toLowerCase
    WebDavFile(new URL("file://" + file.getPath), file.getName, file.length / 1024,
      isDirectory = false,
      isPlainText = name.endsWith(".txt"),
      isDataFile = name.endsWith(".csv") || name.endsWith(".zip"))
  }
}
