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

import com.github.sardine.{DavResource, Sardine}
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.JavaConverters._

class WebdavFinder(logger: SimpleLogger) {

  def findAvailable(url: String, username: String, password: String): List[OSGBProduct] = {
    val file = exploreRemoteTree(url, username, password)
    extractUsableProducts(file)
  }

  private def extractUsableProducts(file: WebDavFile): List[OSGBProduct] = {
    extractUsableProductFor("abi", file) ++ extractUsableProductFor("abp", file)
  }

  private def extractUsableProductFor(product: String, file: WebDavFile): List[OSGBProduct] = {
    val matches = file.files.filter(_.name == product)
    if (matches.isEmpty) Nil
    else {
      matches.head.files.flatMap {
        extractOne(product, _)
      }
    }
  }

  private def extractOne(product: String, e: WebDavFile): Option[OSGBProduct] = {
    val fullFolder = e.files.filter(_.name == "full")
    if (fullFolder.isEmpty) None
    else {
      val possibleDownloads: List[WebDavFile] = filterZipsWithTxt(fullFolder.head.files)
      val possibleUrls = possibleDownloads.map(_.url)
      if (possibleUrls.isEmpty) None
      else Some(OSGBProduct(product, e.name.toInt, possibleUrls))
    }
  }

  private def filterZipsWithTxt(files: List[WebDavFile]): List[WebDavFile] = {
    val zipFiles: List[WebDavFile] = files.filter(_.isZipFile)
    val names: List[String] = zipFiles.map(f => removeExtn(f.name))
    val allTxtFilesExist = names.forall(n => files.exists(_.isPlainText))
    if (allTxtFilesExist) zipFiles else Nil
  }

  private def removeExtn(file: String): String = {
    val dot = file.lastIndexOf('.')
    file.substring(0, dot)
  }

  def exploreRemoteTree(url: String, username: String, password: String): WebDavFile = {
    val sardine = begin(username, password)
    val u = new URL(url)
    val s = u.getProtocol + "://" + u.getAuthority
    exploreRemoteTree(s, url, sardine)
  }

  private def exploreRemoteTree(base: String, url: String, sardine: Sardine): WebDavFile = {
    val url1 = new URL(url)
    var result = WebDavFile(url1, "", true, false, false, Nil)
    val buffer = new scala.collection.mutable.ListBuffer[WebDavFile]()

    val resources: List[DavResource] = sardine.list(url).asScala.toList
    for (res <- resources) {
      val u = base + res.getHref
      if (u == url && res.isDirectory) {
        result = WebDavFile(url1, res.getName, res.isDirectory, false, false, Nil)
      } else if (res.isDirectory) {
        val x = base + res.getPath
        buffer += exploreRemoteTree(base, x, sardine)
      } else {
        buffer += WebDavFile(new URL(u), res.getName, res.isDirectory,
          res.getContentType == "text/plain",
          res.getContentType == "application/zip",
          Nil)
      }
    }

    result.copy(files = buffer.toList)
  }


  // test seam
  def begin(username: String, password: String): Sardine = {
    com.github.sardine.SardineFactory.begin(username, password)
  }
}


case class WebDavFile(url: URL, name: String, isDirectory: Boolean, isPlainText: Boolean, isZipFile: Boolean, files: List[WebDavFile]) {
  override def toString: String = indentedString("")

  private def indentedString(i: String): String = {
    val slash = if (isDirectory) "/" else ""
    val star = if (isZipFile) " +++" else ""
    s"$i$name$slash$star" +
      files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
  }
}


case class OSGBProduct(productName: String, epoch: Int, zips: List[URL])

