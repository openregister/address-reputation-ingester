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
import java.nio.file.Files

import fetch.WebDavFile
import play.api.mvc.{Action, AnyContent, Request, Result}
import services.exec.WorkQueue
import uk.gov.hmrc.play.microservice.controller.BaseController

import scala.annotation.tailrec
import scala.io.Source

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

  def showLog(): Action[AnyContent] = Action {
    request => {
      Ok(readLogFile(Option(new File(".")))).withHeaders(CONTENT_TYPE -> "text/plain")
    }
  }

  private val logFile1 = "address-reputation-ingester.log"
  private val logFile2 = "application.log"
  private val logFileOptions = Seq(logFile1, logFile2)

  private def readLogFile(dir: Option[File]) = {
    val logDir = findLogDir(dir)
    val found = if (logDir.nonEmpty) logFileOptions.map(new File(logDir.get, _)).find(_.exists) else None
    if (found.nonEmpty) {
      found.get.getPath + "\n" +
        Source.fromFile(found.get).mkString
    } else {
      val pwd = new File(".").getCanonicalFile
      "Log file not found in " + pwd
    }
  }

  @tailrec
  private def findLogDir(dir: Option[File]): Option[File] = {
    if (dir.isEmpty) None
    else {
      val logDir = new File(dir.get, "logs")
      if (logDir.exists) Some(logDir)
      else findLogDir(Option(dir.get.getParentFile))
    }
  }

  def dirTree(): Action[AnyContent] = Action {
    request => {
      val tree = listFiles(ControllerConfig.downloadFolder)
      val disk = reportDiskSpace(ControllerConfig.downloadFolder)
      val pwd = System.getenv("PWD")
      val body = s"${ControllerConfig.downloadFolder}\n$tree\n$disk\nPWD=$pwd"
      Ok(body).withHeaders(CONTENT_TYPE -> "text/plain")
    }
  }

  private def reportDiskSpace(dir: File): String = {
    def memSize(size: Long): String = {
      val kb = size / 1024
      val mb = kb / 1024
      val gb = mb / 1024
      if (gb >= 100) "%d GiB".format(gb)
      else if (mb >= 100) "%d MiB".format(mb)
      else "%d KiB".format(kb)
    }
    try {
      val downloadFileStore = Files.getFileStore(ControllerConfig.downloadFolder.toPath)
      val total = downloadFileStore.getTotalSpace
      val usable = downloadFileStore.getUsableSpace
      "Disk space free %s out of total %s.".format(memSize(usable), memSize(total))
    } catch {
      // includes i/o and security manager exception, etc
      case e: Exception =>
        s"Disk space is unknown (${e.getMessage})."
    }
  }

  private def listFiles(dir: File): WebDavFile = {
    val list = Option(dir.listFiles)
    if (list.isEmpty) {
      toWebDavFile(dir)
    }
    else {
      val files = list.get.toList.sorted
      val (dirs, plains) = files.partition(_.isDirectory)
      val subs = dirs.map(listFiles)
      val others = plains.map(f => toWebDavFile(f))
      toWebDavFile(dir, subs ++ others)
    }
  }

  private def toWebDavFile(file: File, contains: List[WebDavFile] = Nil): WebDavFile = {
    val url = new URL("file://" + file.getPath)
    val name = file.getName.toLowerCase
    WebDavFile(url, file.getName, length(file),
      isDirectory = file.isDirectory,
      isPlainText = name.endsWith(".txt"),
      isDataFile = name.endsWith(".csv") || name.endsWith(".zip"),
      files = contains)
  }

  private def length(file: File): Long = {
    try {
      if (file.isDirectory) 0L else file.length / 1024
    } catch {
      case se: SecurityException => 0L
    }
  }
}
