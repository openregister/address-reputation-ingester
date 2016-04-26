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

package services.ingester.fetch

import java.net.URL
import java.nio.file._

import com.github.sardine.{DavResource, Sardine}
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.JavaConverters._

class WebdavFetcher(logger: SimpleLogger, factory: SardineFactory2) {

  // Downloads a specified set of remote files, marks them all with a completion marker (.done),
  // then returns the total bytes copied.
  def fetchList(product: OSGBProduct, username: String, password: String, outputDirectory: Path): Long = {
    val sardine = factory.begin(username, password)
    val sizes = product.zips.map {
      webDavFile =>
        fetchFile(webDavFile.url, sardine, outputDirectory)
    }
    sizes.sum
  }

  // Searches for remote files, downloads them, marks them all with a completion marker (.done),
  // then returns the total bytes copied.
  // Note that this doesn't check the existence of completion marker files on the remote server.
  def fetchAll(url: String, username: String, password: String, outputDirectory: Path): Long = {
    if (!Files.exists(outputDirectory)) {
      Files.createDirectories(outputDirectory)
    }
    val sardine = factory.begin(username, password)
    val resources = sardine.list(url).asScala
    val sizes = resources.filterNot(_.isDirectory).map {
      res =>
        val absoluteUrl = toUrl(url, res)
        fetchFile(absoluteUrl, sardine, outputDirectory)
    }
    sizes.sum
  }

  private def toUrl(base: String, res: DavResource): URL = {
    val myUrl = new URL(base)
    new URL(myUrl.getProtocol, myUrl.getHost, myUrl.getPort, res.getHref.getPath)
  }

  private def fetchFile(url: URL, sardine: Sardine, outputDirectory: Path): Long = {
    val file = fileOf(url)
    val in = sardine.get(url.toExternalForm)
    try {
      val out = outputDirectory.resolve(file)
      val bytesCopied = Files.copy(in, out)
      Files.createFile(outputDirectory.resolve(file + ".done"))
      logger.info("Fetched " + file)
      bytesCopied
    } finally {
      in.close()
    }
  }

  private def fileOf(url: URL): String = {
    val file = url.getPath
    val slash = file.lastIndexOf('/')
    file.substring(slash + 1)
  }
}

