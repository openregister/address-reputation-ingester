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

import java.net.{ProxySelector, URL}

import com.github.sardine.{DavResource, Sardine}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.AuthSchemes
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClientBuilder}

import scala.collection.JavaConverters._

class SardineWrapper(val url: URL, username: String, password: String, proxyAuthInfo: Option[SardineAuthInfo], factory: SardineFactory2) {

  def begin: Sardine = factory.begin(targetAuthInfo, proxyAuthInfo)

  def exploreRemoteTree: WebDavTree = {
    val sardine = factory.begin(targetAuthInfo, proxyAuthInfo)
    val s = url.getProtocol + "://" + url.getAuthority
    WebDavTree(exploreRemoteTree(s, url, sardine))
  }

  private def targetAuthInfo: SardineAuthInfo = SardineAuthInfo(url.getHost, url.getPort, username, password)

  private def exploreRemoteTree(base: String, url: URL, sardine: Sardine): WebDavFile = {
    val href = url.toString
    var result = WebDavFile(url, "", 0L, true, false, false, Nil)
    val buffer = new scala.collection.mutable.ListBuffer[WebDavFile]()

    val resources: List[DavResource] = sardine.list(href).asScala.toList.sortWith(davResOrder)
    for (res <- resources) {
      val u = base + res.getHref
      if (u == href && res.isDirectory) {
        result = WebDavFile(url, res.getName, 0L, res.isDirectory, false, false, Nil)
      } else if (res.isDirectory) {
        buffer += exploreRemoteTree(base, new URL(u), sardine)
      } else {
        buffer += WebDavFile(new URL(u), res.getName, res.getContentLength / 1024, res.isDirectory, isTxtFile(res), isDataFile(res), Nil)
      }
    }

    result.copy(files = buffer.toList)
  }

  private def davResOrder(a: DavResource, b: DavResource) = (a.getHref compareTo b.getHref) < 0

  private def isTxtFile(res: DavResource): Boolean = {
    res.getContentType == "text/plain" ||
      (SardineWrapper.binaryMimeTypes.contains(res.getContentType) && extn(res.getName) == ".txt")
  }

  private def isDataFile(res: DavResource): Boolean = {
    res.getContentType == "application/zip" ||
      res.getContentType == "text/csv" ||
      (SardineWrapper.binaryMimeTypes.contains(res.getContentType) && SardineWrapper.dataFileExtensions.contains(extn(res.getName)))
  }

  private def extn(filename: String) = {
    val dot = filename.lastIndexOf('.')
    if (dot < 0) ""
    else filename.substring(dot).toLowerCase
  }
}


object SardineWrapper {
  val binaryMimeTypes = Set("application/octet-stream", "application/binary")
  val dataFileExtensions = Set(".zip", ".csv")
}


class SardineFactory2 {
  def begin(targetAuthInfo: SardineAuthInfo, proxyAuthInfo: Option[SardineAuthInfo]): Sardine = {
    new SardineImpl2(targetAuthInfo, proxyAuthInfo)
  }
}

class SardineImpl2(targetAuthInfo: SardineAuthInfo, proxyAuthInfo: Option[SardineAuthInfo])
  extends com.github.sardine.impl.SardineImpl() {

  protected override def configure(selector: ProxySelector, credentials: CredentialsProvider): HttpClientBuilder = {
    val cp: CredentialsProvider = new BasicCredentialsProvider

    cp.setCredentials(targetAuthInfo.authscope, targetAuthInfo.credentials)

    proxyAuthInfo match {
      case Some(a) => cp.setCredentials(a.authscope, a.credentials)
      case None =>
    }

    super.configure(selector, credentials).setDefaultCredentialsProvider(cp)
  }
}

case class SardineAuthInfo(host: String, port: Int, user: String, password: String) {
  def authscope = new AuthScope(host, port, AuthScope.ANY_REALM, AuthSchemes.BASIC)

  def credentials = new UsernamePasswordCredentials(user, password)
}
