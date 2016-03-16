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

package services.addressimporter.ftpdownloader


import java.io.File

import org.apache.commons.net.ftp.FTPConnectionClosedException

import scala.util.{Failure, Success, Try}

trait FtpDownloader {
  fileIO: FileIO =>

  val DefaultFileCreationDelay = 86400000L

  var FileCreationDelay: Long = DefaultFileCreationDelay

  def ftpDownload(currentFolder: String, localTemp: String): Try[(Boolean, String)] = {

    def downloadFolder(currentFolder: String): Try[(Boolean, String)] = {
      Try {
        val (_, subFolder) = downloadFiles(currentFolder, localTemp, System.currentTimeMillis()).get

        val foldersTry: Try[List[String]] = fileIO.folder(currentFolder)

        foldersTry match {
          case Success(folders) =>
            val x = folders.map { aFolder => downloadFolder(currentFolder + "/" + aFolder) }

            if (x.exists(_.isFailure)) {
              val a = x.filter(_.isFailure).head
              throw new FTPConnectionClosedException(a.toString)
            } else if (x.isEmpty) (true, subFolder) else x.head.get

          case Failure(err) => throw err
        }
      }
    }

    downloadFolder(currentFolder)
  }

  def oldFiles(now: Long, files: List[OsFile]): List[OsFile] = {
    files.filter(f => !withinValidTime(f.date, now))
  }

  def downloadFiles(osSource: String, localFolderName: String, time: Long): Try[(Int, String)] = Try {

    val filesTry = fileIO.files(osSource)

    val grandTotal = filesTry match {
      case Success(files) =>

        val oFiles = oldFiles(time, files)

        val totalToDownload = files.size

        val subFolderName: String = folderName(oFiles).getOrElse("Unknown")

        val totalThatWereDownloaded = oFiles.foldLeft(0) {
          case (count, f) =>
            val result = downloadAFile(f, localFolderName + "/" + subFolderName).get
            if (result) count + 1 else count
        }
        if (totalThatWereDownloaded != totalToDownload) {
          val missing = totalToDownload - totalThatWereDownloaded
          // TODO better information would be given by first catching the case where none were downloaded.
          throw new UnfinishedDownloadException(s"Not all were downloaded: missing $missing out of $totalToDownload")
        }
        (totalThatWereDownloaded, subFolderName)

      case Failure(err) =>
        throw err
        (0, "")
    }
    grandTotal
  }

  def folderName(osfiles: List[OsFile]): Option[String] = Try {
    val f = osfiles.head.name
    if (f.endsWith(".zip") && f.startsWith("AddressBasePremium")) {
      Some(f.split("_")(2))
    }
    else None
  }.recover { case err => None }.get


  def downloadAFile(osFile: OsFile, localFolderName: String): Try[Boolean] = Try {
    val localFileName = localFolderName + "/" + osFile.name
    fileIO.createLocalFolder(localFolderName)
    if (alreadyDownloaded(localFileName, osFile.size).get) true
    else fileIO.retrieveFile(osFile.folder + "/" + osFile.name, localFileName).get
  }

  def withinValidTime(fileCreationTime: Long, timeNow: Long): Boolean = {
    (timeNow - fileCreationTime) < FileCreationDelay
  }


  def alreadyDownloaded(fileName: String, size: Long): Try[Boolean] = Try {
    val f = new File(fileName)

    if (!f.exists()) false
    else {
      // does exist
      if (f.getTotalSpace == size) true
      else false
    }
  }
}

class UnfinishedDownloadException(msg: String) extends Exception(msg)

case class OsFile(name: String, folder: String, date: Long, size: Long)

trait FileIO extends OsFtp with LocalFileSystem

trait OsFtp {

  val DefaultFtpPort = 21

  def login(username: String, password: String, server: String, port: Int = DefaultFtpPort): Try[Boolean]

  def folder(dir: String): Try[List[String]]

  def files(dir: String): Try[List[OsFile]]

  def retrieveFile(from: String, to: String): Try[Boolean]
}

trait LocalFileSystem {
  def createLocalFolder(dir: String): Try[Unit]

}


