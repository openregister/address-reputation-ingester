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

import java.io.{FileOutputStream, BufferedOutputStream, File}
import org.apache.commons.net.ftp.{FTP, FTPClient}
import scala.util.Try
import play.api.Logger


trait RealWorldIO extends FileIO {

  val ftpClient = new FTPClient()

  def login(username: String, password: String, server: String, port: Int = DefaultFtpPort): Try[Boolean] = {
    Try {
      ftpClient.connect(server, port)
      ftpClient.login(username, password)
      ftpClient.enterLocalPassiveMode()
      ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
      true
    }
  }

  override def folder(dir: String): Try[List[String]] = Try {
    val listFolders = ftpClient.listDirectories(dir)
    val folderNames = listFolders.map(_.getName)
    folderNames.toList
  }


  override def files(dir: String): Try[List[OsFile]] = Try {
    val files = ftpClient.listFiles(dir)
    files.filter(_.isFile).map(f => OsFile(f.getName, dir, f.getTimestamp.getTimeInMillis, f.getSize)).toList
  }

  override def retrieveFile(from: String, to: String): Try[Boolean] = Try {
    Logger("RealWorldIO").info(s">>retrieveFile from: $from  to:  $to")
    val downloadFile1 = new File(to)
    val outputStream1 = new BufferedOutputStream(new FileOutputStream(downloadFile1))
    val success = ftpClient.retrieveFile(from, outputStream1)
    outputStream1.close()
    success
  }


  override def createLocalFolder(dir: String): Try[Unit] = Try {
    val file = new java.io.File(dir)
    if (!file.isDirectory) {
      file.mkdirs
    }
  }
}
