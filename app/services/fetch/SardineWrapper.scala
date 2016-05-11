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

import java.net.URL

import com.github.sardine.{DavResource, Sardine}
import services.model.StatusLogger

import scala.collection.JavaConverters._

class SardineWrapper(val url: URL, username: String, password: String, logger: StatusLogger, factory: SardineFactory2) {

  def begin: Sardine = factory.begin(username, password)

  def exploreRemoteTree: WebDavTree = {
    val sardine = factory.begin(username, password)
    val s = url.getProtocol + "://" + url.getAuthority
    WebDavTree(exploreRemoteTree(s, url, sardine))
  }

  private def exploreRemoteTree(base: String, url: URL, sardine: Sardine): WebDavFile = {
    val href = url.toString
    var result = WebDavFile(url, "", true, false, false, Nil)
    val buffer = new scala.collection.mutable.ListBuffer[WebDavFile]()

    val resources: List[DavResource] = sardine.list(href).asScala.toList
    for (res <- resources) {
      val u = base + res.getHref
      if (u == href && res.isDirectory) {
        result = WebDavFile(url, res.getName, res.isDirectory, false, false, Nil)
      } else if (res.isDirectory) {
        val x = base + res.getPath
        buffer += exploreRemoteTree(base, new URL(x), sardine)
      } else {
        buffer += WebDavFile(new URL(u), res.getName, res.isDirectory, isTxtFile(res), isZipFile(res), Nil)
      }
    }

    result.copy(files = buffer.toList)
  }

  private def isTxtFile(res: DavResource): Boolean = {
    res.getContentType == "text/plain" ||
      (res.getContentType == "application/octet-stream" && extn(res.getName) == ".txt")
  }

  private def isZipFile(res: DavResource): Boolean = {
    res.getContentType == "application/zip" ||
      (res.getContentType == "application/octet-stream" && extn(res.getName) == ".zip")
  }

  private def extn(filename: String) = {
    val dot = filename.lastIndexOf('.')
    if (dot < 0) ""
    else filename.substring(dot)
  }
}


class SardineFactory2 {
  def begin(username: String, password: String): Sardine = {
    com.github.sardine.SardineFactory.begin(username, password)
  }
}


