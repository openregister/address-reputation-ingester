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
import java.util
import javax.xml.namespace.QName

import com.github.sardine.{DavResource, Sardine}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito
import uk.co.hmrc.logging.StubLogger

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class WebdavFinderTest extends PlaySpec with Mockito {

  import StubDavResource._

  class Context {
    val logger = new StubLogger()
    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineFactory2]
    when(sardineFactory.begin("foo", "bar")) thenReturn sardine

    val root = "http://somedavserver.com/webdav"
    val productResources = List[DavResource](
      dir("/webdav/", "webdav"),
      dir("/webdav/abi/", "abi"),
      dir("/webdav/abp/", "abp")
    )
    val abiEpochResources = List[DavResource](
      dir("/webdav/abi/", "abi")
    )
    val abpEpochResources = List[DavResource](
      dir("/webdav/abp/", "abp"),
      dir("/webdav/abp/37/", "37"),
      dir("/webdav/abp/38/", "38"),
      dir("/webdav/abp/39/", "39"),
      dir("/webdav/abp/40/", "40")
    )
    val abpE37VariantResources = List[DavResource](
      dir("/webdav/abp/37/", "37"),
      dir("/webdav/abp/37/full/", "full")
    )
    val abpE38VariantResources = List[DavResource](
      dir("/webdav/abp/38/", "38"),
      dir("/webdav/abp/38/full/", "full")
    )
    val abpE39VariantResources = List[DavResource](
      dir("/webdav/abp/39/", "39"),
      dir("/webdav/abp/39/full/", "full")
    )
    val abpE40VariantResources = List[DavResource](
      dir("/webdav/abp/40/", "40"),
      dir("/webdav/abp/40/full/", "full")
    )
    val file37Resources = List[DavResource](
      dir("/webdav/abp/37/full/", "full"),
      file("/webdav/abp/37/full/DVD1.txt", "DVD1.txt", "text/plain")
    )
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
    val badFile39Resources = List[DavResource](
      dir("/webdav/abp/39/full/", "full"),
      file("/webdav/abp/39/full/DVD1.zip", "DVD1.zip", "application/zip"),
      file("/webdav/abp/39/full/DVD2.txt", "DVD2.txt", "text/plain")
    )
    val file40Resources = List[DavResource](
      dir("/webdav/abp/40/full/", "full"),
      file("/webdav/abp/40/full/DVD1.zip", "DVD1.zip", "application/zip"),
      file("/webdav/abp/40/full/DVD2.zip", "DVD2.zip", "application/zip")
    )
  }

  "find available" should {
    """
      discover one products with two epochs
      and ignore any unimportant files
    """ in {
      new Context {
        // given
        when(sardine.list(root + "/")) thenReturn productResources.asJava
        when(sardine.list(root + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(root + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(root + "/abp/37/")) thenReturn abpE37VariantResources.asJava
        when(sardine.list(root + "/abp/38/")) thenReturn abpE38VariantResources.asJava
        when(sardine.list(root + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(root + "/abp/40/")) thenReturn abpE40VariantResources.asJava
        when(sardine.list(root + "/abp/37/full/")) thenReturn file37Resources.asJava
        when(sardine.list(root + "/abp/38/full/")) thenReturn file38Resources.asJava
        when(sardine.list(root + "/abp/39/full/")) thenReturn file39Resources.asJava
        when(sardine.list(root + "/abp/40/full/")) thenReturn file40Resources.asJava
        val finder = new WebdavFinder(logger, new SardineWrapper(logger, sardineFactory))
        // when
        val list = finder.findAvailable(root + "/", "foo", "bar")
        // then
        list must be(List(
          OSGBProduct("abp", 38, List(new URL(root + "/abp/38/full/DVD1.zip"))),
          OSGBProduct("abp", 39, List(new URL(root + "/abp/39/full/DVD1.zip"), new URL(root + "/abp/39/full/DVD2.zip")))
        ))
        // finally
      }
    }

    """
      discover one products with one epoch
      and ignore any unimportant files
    """ in {
      new Context {
        // given
        when(sardine.list(root + "/")) thenReturn productResources.asJava
        when(sardine.list(root + "/abi/")) thenReturn abiEpochResources.asJava
        when(sardine.list(root + "/abp/")) thenReturn abpEpochResources.asJava
        when(sardine.list(root + "/abp/37/")) thenReturn abpE37VariantResources.asJava
        when(sardine.list(root + "/abp/38/")) thenReturn abpE38VariantResources.asJava
        when(sardine.list(root + "/abp/39/")) thenReturn abpE39VariantResources.asJava
        when(sardine.list(root + "/abp/40/")) thenReturn abpE40VariantResources.asJava
        when(sardine.list(root + "/abp/37/full/")) thenReturn file37Resources.asJava
        when(sardine.list(root + "/abp/38/full/")) thenReturn file38Resources.asJava
        when(sardine.list(root + "/abp/39/full/")) thenReturn badFile39Resources.asJava
        when(sardine.list(root + "/abp/40/full/")) thenReturn file40Resources.asJava
        val finder = new WebdavFinder(logger, new SardineWrapper(logger, sardineFactory))
        // when
        val list = finder.findAvailable(root + "/", "foo", "bar")
        // then
        list must be(List(
          OSGBProduct("abp", 38, List(new URL(root + "/abp/38/full/DVD1.zip")))
        ))
        // finally
      }
    }
  }

}


class StubDavResource(href: String, creation: util.Date, modified: util.Date, contentType: String, contentLength: Long,
                      etag: String, displayName: String, resourceTypes: util.List[QName],
                      contentLanguage: String, supportedReports: util.List[QName],
                      customProps: util.Map[QName, String])
  extends DavResource(href, creation, modified,
    contentType, contentLength,
    etag, displayName, resourceTypes,
    contentLanguage, supportedReports,
    customProps)

object StubDavResource {
  def dir(href: String, name: String): StubDavResource = file(href, name, "httpd/unix-directory")

  def file(href: String, name: String, contentType: String): StubDavResource = {
    val now = new util.Date()
    new StubDavResource(href, now, now, contentType, 123, "etag", name,
      Nil.asJava, "en", Nil.asJava, Map[QName, String]().asJava)
  }
}

