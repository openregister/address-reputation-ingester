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

package fetch

import java.io.File
import java.net.URL
import java.nio.file._

import com.github.sardine.{DavResource, Sardine}
import services.exec.Continuer
import services.model.StatusLogger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

class WebdavFetcher(factory: SardineWrapper, val downloadFolder: File, status: StatusLogger) {

  // Downloads a specified set of remote files, marks them all with a completion marker (.done),
  // then returns the total bytes copied.
  def fetchList(product: OSGBProduct, outputPath: String, continuer: Continuer, forceFetch: Boolean): List[DownloadItem] = {
    status.info("Product bundle: {}", product)
    val outputDirectory = resolveAndMkdirs(outputPath)
    val sardine = factory.begin
    for (webDavFile <- product.zips
         if continuer.isBusy) yield {
      fetchFile(webDavFile.url, sardine, outputDirectory, forceFetch)
    }
  }

  private def toUrl(base: String, res: DavResource): URL = {
    val myUrl = new URL(base)
    new URL(myUrl.getProtocol, myUrl.getHost, myUrl.getPort, res.getHref.getPath)
  }

  private def fetchFile(url: URL, sardine: Sardine, outputDirectory: File, forceFetch: Boolean): DownloadItem = {
    val file = fileOf(url)
    val outFile = DownloadedFile(outputDirectory, file)
    if (forceFetch || !outFile.exists || outFile.isIncomplete) {
      // pre-existing files are considered incomplete
      outFile.delete()

      val ff = if (forceFetch) " (forced)" else ""
      status.info(s"Fetching {} to $file$ff.", url)
      val dt = new DiagnosticTimer
      val fetched = doFetchFile(url, sardine, outFile)
      outFile.touchDoneFile()
      status.info(s"Fetched $file in {}.", dt)
      DownloadItem.fresh(fetched)

    } else {
      status.info(s"Already had $file.")
      DownloadItem.stale(outFile)
    }
  }

  private def doFetchFile(url: URL, sardine: Sardine, outFile: DownloadedFile): DownloadedFile = {
    val in = sardine.get(url.toExternalForm)
    try {
      Files.copy(in, outFile.toPath)
      outFile
    } finally {
      in.close()
    }
  }

  private def fileOf(url: URL): String = {
    val file = url.getPath
    val slash = file.lastIndexOf('/')
    file.substring(slash + 1)
  }

  private def resolveAndMkdirs(outputPath: String): File = {
    val outputDirectory = if (outputPath.nonEmpty) new File(downloadFolder, outputPath) else downloadFolder
    outputDirectory.mkdirs()
    outputDirectory
  }
}
