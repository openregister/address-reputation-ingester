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

import java.io.{File, IOException}
import java.net.URL
import java.nio.file.Files
import java.text.DateFormat
import java.util.Date

import config.JacksonMapper
import fetch.WebDavFile
import play.api.http.MimeTypes
import play.api.mvc._
import services.exec.{TaskInfo, WorkQueue}
import uk.gov.hmrc.play.microservice.controller.BaseController

import scala.io.Source


object AdminController extends AdminController(
  ControllerConfig.authAction,
  WorkQueue.singleton,
  ControllerConfig.realmString)


class AdminController(action: ActionBuilder[Request],
                      worker: WorkQueue,
                      realmString: String) extends BaseController {

  private val PlainText = CONTENT_TYPE -> MimeTypes.TEXT
  private val ApplJson = CONTENT_TYPE -> MimeTypes.JSON
  private val startTime = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(new Date())

  def index: Action[AnyContent] = action {
    Redirect("ui/index.html")
  }

  def cancelTask(): Action[AnyContent] = action {
    if (worker.abort()) {
      Accepted
    } else {
      Ok("Nothing to cancel").as(MimeTypes.TEXT)
    }
  }

  def cancelQueue(id: Int): Action[AnyContent] = action {
    Ok(JacksonMapper.writeValueAsString(worker.dropQueueItem(id))).as(MimeTypes.JSON)
  }

  // widely used in tests - so this is *not* basic-auth protected
  def status(): Action[AnyContent] = Action {
    Ok(worker.status).as(MimeTypes.TEXT)
  }

  def fullStatus(): Action[AnyContent] = action {
    Ok(worker.fullStatus).as(MimeTypes.TEXT)
  }

  def viewQueue(): Action[AnyContent] = action {
    Ok(JacksonMapper.writeValueAsString(worker.viewQueue)).as(MimeTypes.JSON)
  }

  def showLog(dir: Option[String]): Action[AnyContent] = action {
    val d = if (dir.isEmpty) "." else dir.get
    Ok(LogFileHelper.readLogFile(new File(d).getCanonicalFile)).as(MimeTypes.TEXT)
  }

  def dirTree(root: Option[String], max: Option[Int]): Action[AnyContent] = action {
    val dir = if (root.isDefined) new File(root.get) else ControllerConfig.downloadFolder
    val treeInfo = DirTreeHelper.dirTreeInfo(dir, max getOrElse 2)
    Ok(treeInfo).as(MimeTypes.TEXT)
  }

  def realm: Action[AnyContent] = action {
    Ok(s"$realmString\n$startTime").as(MimeTypes.TEXT)
  }
}


object LogFileHelper {
  private val logFile1 = "address-reputation-ingester.log"
  private val logFile2 = "application.log"

  def readLogFile(dir: File): String = {
    val found = testForLogDir(dir, logFile1) orElse testForLogDir(dir, logFile2)
    if (found.nonEmpty) {
      val path = found.get.getPath
      try {
        path + "\n" +
          Source.fromFile(found.get).mkString
      } catch {
        case se: SecurityException =>
          s"Access to $path is forbidden (${se.getMessage})"
        case io: IOException =>
          s"Access to $path failed (${io.getMessage})"
      }
    } else {
      "Log file not found in " + dir
    }
  }

  private def testForLogDir(dir: File, name: String): Option[File] = {
    val file1 = new File(dir, name)
    val file2 = new File(dir, "logs/" + name)
    if (file1.exists) Some(file1)
    else if (file2.exists) Some(file2)
    else None
  }
}


object DirTreeHelper {
  def dirTreeInfo(dir: File, maxDepth: Int): String = {
    val tree = DirTreeHelper.listFiles(dir, maxDepth)
    val disk = DirTreeHelper.reportDiskSpace(dir)
    val pwd = System.getenv("PWD")
    s"root=$dir (max=$maxDepth)\n$tree\n$disk\nPWD=$pwd"
  }

  def reportDiskSpace(dir: File): String = {
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

  def listFiles(dir: File, remainingDepth: Int): WebDavFile = {
    val list = Option(dir.listFiles)
    if (list.isEmpty || remainingDepth == 0) {
      toWebDavFile(dir)
    }
    else {
      val files = list.get.toList.sorted
      val (dirs, plains) = files.partition(_.isDirectory)
      val subs = dirs.map(listFiles(_, remainingDepth - 1))
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
