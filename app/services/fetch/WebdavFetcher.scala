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

package services.fetch

import java.io.File
import java.net.URL
import java.nio.file._

import com.github.sardine.{DavResource, Sardine}
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.JavaConverters._

class WebdavFetcher(logger: SimpleLogger, factory: SardineWrapper, downloadFolder: File) {

  // Downloads a specified set of remote files, marks them all with a completion marker (.done),
  // then returns the total bytes copied.
  def fetchList(product: OSGBProduct, outputPath: String): List[File] = {
    val outputDirectory = resolveAndMkdirs(outputPath)
    val sardine = factory.begin
    product.zips.map {
      webDavFile =>
        fetchFile(webDavFile.url, sardine, outputDirectory)
    }
  }

  // Searches for remote files, downloads them, marks them all with a completion marker (.done),
  // then returns the total bytes copied.
  // Note that this doesn't check the existence of completion marker files on the remote server.
  def fetchAll(url: String, outputPath: String): List[File] = {
    val outputDirectory = resolveAndMkdirs(outputPath)
    val sardine = factory.begin
    logger.info("Listing {}", url)
    val resources = sardine.list(url).asScala.toList.filterNot(_.isDirectory)
    resources.map {
      res =>
        val absoluteUrl = toUrl(url, res)
        fetchFile(absoluteUrl, sardine, outputDirectory)
    }
  }

  private def toUrl(base: String, res: DavResource): URL = {
    val myUrl = new URL(base)
    new URL(myUrl.getProtocol, myUrl.getHost, myUrl.getPort, res.getHref.getPath)
  }

  private def fetchFile(url: URL, sardine: Sardine, outputDirectory: File): File = {
    val file = fileOf(url)
    val outFile = new File(outputDirectory, file)
    val doneFile = new File(outputDirectory, file + ".done")
    if (outFile.exists() && doneFile.exists() && outFile.lastModified() <= doneFile.lastModified()) {
      logger.info(s"Already had $file")

    } else {
      // pre-existing files are considered incomplete
      outFile.delete()
      doneFile.delete()

      logger.info(s"Fetching {} to $file", url)
      val dt = new DiagnosticTimer
      doFetchFile(url, sardine, outFile)
      Files.createFile(doneFile.toPath)
      logger.info(s"Fetched $file in {}", dt)
    }
    outFile
  }

  private def doFetchFile(url: URL, sardine: Sardine, outFile: File): File = {
    val file = fileOf(url)
    val in = sardine.get(url.toExternalForm)
    try {
      val bytesCopied = Files.copy(in, outFile.toPath)
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

