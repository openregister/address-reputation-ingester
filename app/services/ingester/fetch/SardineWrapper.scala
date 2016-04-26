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

class SardineWrapper(logger: SimpleLogger, factory: SardineFactory2) {

  def exploreRemoteTree(url: String, username: String, password: String): WebDavFile = {
    val sardine = factory.begin(username, password)
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
}


case class WebDavFile(url: URL, fullName: String,
                      isDirectory: Boolean, isPlainText: Boolean, isZipFile: Boolean,
                      files: List[WebDavFile]) {

  val name = {
    val dot = fullName.lastIndexOf('.')
    if (dot < 0) fullName
    else fullName.substring(0, dot)
  }

  override def toString: String = indentedString("")

  private def indentedString(i: String): String = {
    val slash = if (isDirectory) "/" else ""
    val zip = if (isZipFile) " (zip)" else ""
    s"$i$fullName$slash$zip" +
      files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
  }
}


class SardineFactory2 {
  def begin(username: String, password: String): Sardine = {
    com.github.sardine.SardineFactory.begin(username, password)
  }
}


