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

package controllers

import java.io._
import java.nio.charset.StandardCharsets._

import com.typesafe.config.ConfigFactory
import play.api.libs.iteratee.Enumerator
import play.api.mvc._
import services.addressimporter.ftpdownloader.{FtpDownloader, RealWorldIO}
import uk.co.hmrc.address.osgb.DbAddress

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}


object OrdnanceSurveyController extends OrdnanceSurveyController

trait OrdnanceSurveyController extends Controller {

  import services.addressimporter.converter.Extractor

  import scala.concurrent.ExecutionContext.Implicits.global


  def manualImport(): Action[AnyContent] = Action {

    val out: PipedOutputStream = new PipedOutputStream()

    val data: InputStream = new BufferedInputStream(new PipedInputStream(out))

    val dataContent: Enumerator[Array[Byte]] = Enumerator.fromStream(data).andThen(Enumerator.eof)

    Future {
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

        val ftpClient = new FtpDownloader with RealWorldIO
        ftpClient.FileCreationDelay = 1

        ftpClient.login(user, pass, server).map { result =>
          ftpClient.ftpDownload(osHomeFolder, tmpZipfilesHome) match {
            case Success((count, subFolder)) =>

              val csvOut = (line: DbAddress) => {
                // scalastyle:off
                out.write(line.toString.getBytes(UTF_8))
                out.write('\n')
                out.flush()
              }
              val result = Extractor.extract(new File(tmpZipfilesHome + "/" + subFolder), csvOut)

            case Failure(err) =>
              err.printStackTrace()
          }
        }
        data.close()
        out.flush()
        out.close()
        ftpClient.logout()

      }.recoverWith { case x =>
        // TODO this should not be necessary - just let it bubble up
        x.printStackTrace()
        throw x
      }
    }

    Ok.chunked(dataContent).as("text/csv")
  }
}
