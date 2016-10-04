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

import java.net.URL

import com.github.sardine.{DavResource, Sardine}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito
import services.model.StatusLogger
import uk.gov.hmrc.logging.StubLogger

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class SardineWrapperTest extends PlaySpec with Mockito {

  import StubDavResource._

  val base = "http://somedavserver.com:81/webdav"
  val baseUrl = new URL(base + "/")

  val productResources = List[DavResource](
    dir("/webdav/", "webdav"),
    dir("/webdav/abi/", "abi"),
    dir("/webdav/abp/", "abp"),
    dir("/webdav/Code/", "Code")
  )
  val abiEpochResources = List[DavResource](
    dir("/webdav/abi/", "abi")
  )
  val abpEpochResources = List[DavResource](
    dir("/webdav/abp/", "abp"),
    dir("/webdav/abp/39/", "39"),
    dir("/webdav/abp/38/", "38")
  )
  val abpE38VariantResources = List[DavResource](
    dir("/webdav/abp/38/", "38"),
    dir("/webdav/abp/38/full/", "full")
  )
  val abpE39VariantResources = List[DavResource](
    dir("/webdav/abp/39/", "39"),
    dir("/webdav/abp/39/full/", "full")
  )
  val codeResources = List[DavResource](
    dir("/webdav/Code/", "Code"),
    dir("/webdav/Code/Not%20used/", "Not used") // note the space character
  )
  val codeNotUsedResources = List[DavResource](
    dir("/webdav/Code/Not%20used/", "Not used") // note the space character
  )

  class Context {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineFactory2]
    when(sardineFactory.begin(SardineAuthInfo(baseUrl.getHost, baseUrl.getPort, "username", "password"),None)) thenReturn sardine
  }

  "find available" should {
    "discover a sorted tree of files using standard media types" in {
      new Context {
        // given
        val file38Resources = List[DavResource](
          dir("/webdav/abp/38/full/", "full"),
          file("/webdav/abp/38/full/DVD1.zip", "DVD1.zip", "application/zip"),
          file("/webdav/abp/38/full/DVD1.txt", "DVD1.txt", "text/plain")
        )
        val file39Resources = List[DavResource](
          dir("/webdav/abp/39/full/", "full"),
          file("/webdav/abp/39/full/DVD1.zip", "DVD1.zip", "application/zip"),
          file("/webdav/abp/39/full/DVD1.txt", "DVD1.txt", "text/plain"),
          file("/webdav/abp/39/full/DVD2.zip", "DVD2.zip", "application/zip"),
          file("/webdav/abp/39/full/DVD2.txt", "DVD2.txt", "text/plain")
        )
        when(sardine.list(base + "/")) thenReturn productResources.asJava
        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(base + "/abp/38/")) thenReturn abpE38VariantResources.asJava
        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(base + "/abp/38/full/")) thenReturn file38Resources.asJava
        when(sardine.list(base + "/abp/39/full/")) thenReturn file39Resources.asJava
        when(sardine.list(base + "/Code/")) thenReturn codeResources.asJava
        when(sardine.list(base + "/Code/Not%20used/")) thenReturn codeNotUsedResources.asJava
        val finder = new SardineWrapper(baseUrl, "username", "password", None, sardineFactory)
        // when
        val root = finder.exploreRemoteTree
        // then
        root must be(WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/Code/"), "Code", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/Code/Not%20used/"), "Not used", isDirectory = true)
            )),
            WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true),
            WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/38/"), "38", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/38/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.txt"), "DVD1.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true)
                ))
              )),
              WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/39/full/DVD1.txt"), "DVD1.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD1.zip"), "DVD1.zip", isDataFile = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD2.txt"), "DVD2.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD2.zip"), "DVD2.zip", isDataFile = true)
                ))
              ))
            ))
          ))
        ))
      }
    }

    "discover a sorted tree of files using the file extensions (with zip & txt)" in {
      new Context {
        // given
        val file38Resources = List[DavResource](
          dir("/webdav/abp/38/full/", "full"),
          file("/webdav/abp/38/full/DVD1.zip", "DVD1.zip", "application/octet-stream"),
          file("/webdav/abp/38/full/DVD1.txt", "DVD1.txt", "application/octet-stream")
        )
        val file39Resources = List[DavResource](
          dir("/webdav/abp/39/full/", "full"),
          file("/webdav/abp/39/full/DVD1.zip", "DVD1.zip", "application/octet-stream"),
          file("/webdav/abp/39/full/DVD1.txt", "DVD1.txt", "application/octet-stream"),
          file("/webdav/abp/39/full/DVD2.zip", "DVD2.zip", "application/octet-stream"),
          file("/webdav/abp/39/full/DVD2.txt", "DVD2.txt", "application/octet-stream")
        )
        when(sardine.list(base + "/")) thenReturn productResources.asJava
        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(base + "/abp/38/")) thenReturn abpE38VariantResources.asJava
        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(base + "/abp/38/full/")) thenReturn file38Resources.asJava
        when(sardine.list(base + "/abp/39/full/")) thenReturn file39Resources.asJava
        when(sardine.list(base + "/Code/")) thenReturn codeResources.asJava
        when(sardine.list(base + "/Code/Not%20used/")) thenReturn codeNotUsedResources.asJava
        val finder = new SardineWrapper(baseUrl, "username", "password", None, sardineFactory)
        // when
        val root = finder.exploreRemoteTree
        // then
        root must be(WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/Code/"), "Code", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/Code/Not%20used/"), "Not used", isDirectory = true)
            )),
            WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true),
            WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/38/"), "38", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/38/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.txt"), "DVD1.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true)
                ))
              )),
              WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/39/full/DVD1.txt"), "DVD1.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD1.zip"), "DVD1.zip", isDataFile = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD2.txt"), "DVD2.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/39/full/DVD2.zip"), "DVD2.zip", isDataFile = true)
                ))
              ))
            ))
          ))
        ))
      }
    }

    "discover a sorted tree of files using the file extensions (with csv & txt)" in {
      new Context {
        // given
        // mixture of MIME types because these have been observed on the remote server
        val file39Resources = List[DavResource](
          dir("/webdav/abp/39/full/", "full"),
          file("/webdav/abp/39/full/file001.CSV", "file001.CSV", "application/binary"),
          file("/webdav/abp/39/full/file001.txt", "file001.txt", "application/binary"),
          file("/webdav/abp/39/full/file002.csv", "file002.csv", "application/octet-stream"),
          file("/webdav/abp/39/full/file002.txt", "file002.txt", "application/octet-stream")
        )
        when(sardine.list(base + "/")) thenReturn productResources.asJava
        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(base + "/abp/38/")) thenReturn List[DavResource](dir("/webdav/abp/38/", "38")).asJava
        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(base + "/abp/39/full/")) thenReturn file39Resources.asJava
        when(sardine.list(base + "/Code/")) thenReturn codeResources.asJava
        when(sardine.list(base + "/Code/Not%20used/")) thenReturn codeNotUsedResources.asJava
        val finder = new SardineWrapper(baseUrl, "username", "password", None, sardineFactory)
        // when
        val root = finder.exploreRemoteTree
        // then
        root must be(WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/Code/"), "Code", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/Code/Not%20used/"), "Not used", isDirectory = true)
            )),
            WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true),
            WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/38/"), "38", isDirectory = true, files = Nil),
              WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/39/full/file001.CSV"), "file001.CSV", isDataFile = true),
                  WebDavFile(new URL(base + "/abp/39/full/file001.txt"), "file001.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/39/full/file002.csv"), "file002.csv", isDataFile = true),
                  WebDavFile(new URL(base + "/abp/39/full/file002.txt"), "file002.txt", isPlainText = true)
                ))
              ))
            ))
          ))
        ))
      }
    }

    "discover a sorted tree of files with deeper nesting" in {
      new Context {
        // given
        val abpE39VariantDataResources = List[DavResource](
          dir("/webdav/abp/39/full/", "full"),
          dir("/webdav/abp/39/full/data/", "data")
        )
        val file38Resources = List[DavResource](
          dir("/webdav/abp/38/full/", "full"),
          file("/webdav/abp/38/full/DVD1.zip", "DVD1.zip", "application/zip"),
          file("/webdav/abp/38/full/DVD1.txt", "DVD1.txt", "text/plain")
        )
        val file39Resources = List[DavResource](
          dir("/webdav/abp/39/full/data/", "data"),
          file("/webdav/abp/39/full/data/file001.zip", "file001.zip", "application/zip"),
          file("/webdav/abp/39/full/data/file001.txt", "file001.txt", "text/plain"),
          file("/webdav/abp/39/full/data/file002.zip", "file002.zip", "application/zip"),
          file("/webdav/abp/39/full/data/file002.txt", "file002.txt", "text/plain"),
          file("/webdav/abp/39/full/data/file003.ziP", "file003.ziP", "application/zip"),
          file("/webdav/abp/39/full/data/file003.txt", "file003.txt", "text/plain")
        )
        when(sardine.list(base + "/")) thenReturn productResources.asJava
        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(base + "/abp/38/")) thenReturn abpE38VariantResources.asJava
        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(base + "/abp/38/full/")) thenReturn file38Resources.asJava
        when(sardine.list(base + "/abp/39/full/")) thenReturn abpE39VariantDataResources.asJava
        when(sardine.list(base + "/abp/39/full/data/")) thenReturn file39Resources.asJava
        when(sardine.list(base + "/Code/")) thenReturn codeResources.asJava
        when(sardine.list(base + "/Code/Not%20used/")) thenReturn codeNotUsedResources.asJava
        val finder = new SardineWrapper(baseUrl, "username", "password", None, sardineFactory)
        // when
        val root = finder.exploreRemoteTree
        // then
        root must be(WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/Code/"), "Code", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/Code/Not%20used/"), "Not used", isDirectory = true)
            )),
            WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true),
            WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/38/"), "38", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/38/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.txt"), "DVD1.txt", isPlainText = true),
                  WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true)
                ))
              )),
              WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/abp/39/full/data/"), "data", isDirectory = true, files = List(
                    WebDavFile(new URL(base + "/abp/39/full/data/file001.txt"), "file001.txt", isPlainText = true),
                    WebDavFile(new URL(base + "/abp/39/full/data/file001.zip"), "file001.zip", isDataFile = true),
                    WebDavFile(new URL(base + "/abp/39/full/data/file002.txt"), "file002.txt", isPlainText = true),
                    WebDavFile(new URL(base + "/abp/39/full/data/file002.zip"), "file002.zip", isDataFile = true),
                    WebDavFile(new URL(base + "/abp/39/full/data/file003.txt"), "file003.txt", isPlainText = true),
                    WebDavFile(new URL(base + "/abp/39/full/data/file003.ziP"), "file003.ziP", isDataFile = true)
                  ))
                ))
              ))
            ))
          ))
        ))
      }
    }
  }
}

