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

package services.addressimporter

import java.io._

import play.api.Logger
import com.typesafe.config.ConfigFactory
import services.addressimporter.converter.{CSVLine, Extractor}
import services.addressimporter.ftpdownloader.{FtpDownloader, RealWorldIO}

import scala.util.{Failure, Success, Try}


object Main extends App {

  val logger = Logger("addressimporter-main")

  logger.info("Address Importer")

  val appStart = System.currentTimeMillis()

  Try {
    val conf = ConfigFactory.load()

    // os server details
    val server = conf.getString("app.os.server")
    val port = conf.getString("app.os.port")
    val user = conf.getString("app.os.user")
    val pass = conf.getString("app.os.pass")
    val osHomeFolder = conf.getString("app.os.homeFolder")

    // temp folder details
    val tmpZipfilesHome = conf.getString("app.temp.folder")

    // resulting .csv
    val outCSVFilename = conf.getString("app.output.csvFolder")


    val ftpClient = new FtpDownloader with RealWorldIO
    ftpClient.FileCreationDelay = 1

    ftpClient.login(user, pass, server).map { result =>
      ftpClient.ftpDownload(osHomeFolder, tmpZipfilesHome) match {
        case Success((count, subFolder)) =>
          logger.info(s"Downloaded $count files  into subFolder $subFolder")

          lazy val outCSV = new PrintWriter(new BufferedWriter(new FileWriter(s"$outCSVFilename/addr_$subFolder.csv"), 1024 * 32))

          val csvOut = (out: CSVLine) => {
            // scalastyle:off
            outCSV.println(out.toString)
          }

          val result = Extractor.extract(new File(tmpZipfilesHome + "/" + subFolder), csvOut)
          logger.info("Result: " + result.toString)

          outCSV.flush()
          outCSV.close()

        case Failure(err) =>
          err.printStackTrace()
      }
    }
  }.recoverWith { case x =>
    x.printStackTrace()
    throw x
  }


  val totalTime = (System.currentTimeMillis() - appStart) / 1000

  logger.info(s"Total Execution Time: ${totalTime / 60} mins ${totalTime % 60} secs  ")

}


