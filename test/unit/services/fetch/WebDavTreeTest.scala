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
import java.util
import javax.xml.namespace.QName

import com.github.sardine.DavResource
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class WebDavTreeTest extends PlaySpec {

  val base = "http://somedavserver.com:81/webdav"

  val abiZip39_1 = WebDavFile(new URL(base + "/abi/39/full/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val abiTxt39_1 = WebDavFile(new URL(base + "/abi/39/full/DVD1.txt"), "DVD1.txt", isPlainText = true)

  val abpZip37_1 = WebDavFile(new URL(base + "/abp/37/full/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val abpTxt37_1 = WebDavFile(new URL(base + "/abp/37/full/DVD1.txt"), "DVD1.txt", isPlainText = true)

  val abpZip38_1 = WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val abpTxt38_1 = WebDavFile(new URL(base + "/abp/38/full/DVD1.txt"), "DVD1.txt", isPlainText = true)

  val abpZip39_1 = WebDavFile(new URL(base + "/abp/39/full/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val abpTxt39_1 = WebDavFile(new URL(base + "/abp/39/full/DVD1.txt"), "DVD1.txt", isPlainText = true)

  val abpZip40_1 = WebDavFile(new URL(base + "/abp/40/full/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val abpTxt40_1 = WebDavFile(new URL(base + "/abp/40/full/DVD1.txt"), "DVD1.txt", isPlainText = true)

  val abpZip40_2 = WebDavFile(new URL(base + "/abp/40/full/DVD2.zip"), "DVD2.zip", isZipFile = true)
  val abpTxt40_2 = WebDavFile(new URL(base + "/abp/40/full/DVD2.txt"), "DVD2.txt", isPlainText = true)

  "WebDavTree.name" should {
    "handle name with dot" in {
      abpTxt40_2.name must be("DVD2")
    }

    "handle name without dot" in {
      val dir = WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true)
      dir.name must be("full")
    }
  }

  "find available" should {
    """
      discover one product with two epochs
      and ignore any unimportant files
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                abiZip39_1,
                abiTxt39_1
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                abpZip39_1,
                abpTxt39_1
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                abpZip40_1,
                abpTxt40_1,
                abpZip40_2,
                abpTxt40_2
              ))
            ))
          ))
        )))

      // when
      val list = tree.findAvailableFor("abp")

      // then
      list must be(List(
        OSGBProduct("abp", 39, List(abpZip39_1)),
        OSGBProduct("abp", 40, List(abpZip40_1, abpZip40_2))
      ))
    }
  }

  "find latest" should {
    """
      discover one product with one epoch
      and ignore any unimportant files
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                abiZip39_1,
                abiTxt39_1
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                abpZip39_1,
                abpTxt39_1
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                abpZip40_1,
                abpTxt40_1,
                abpZip40_2,
                abpTxt40_2
              ))
            ))
          ))
        )))

      // when
      val list = tree.findLatestFor("abp")

      // then
      list must be(Some(OSGBProduct("abp", 40, List(abpZip40_1, abpZip40_2))))
    }

    //    """
    //      discover one product with one epoch
    //      and ignore any unimportant files
    //    """ in {
    //      new Context {
    //        // given
    //        when(sardine.list(base + "/")) thenReturn productResources.asJava
    //        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
    //        when(sardine.list(base + "/abi/40/")) thenReturn abiE40VariantResources.asJava
    //        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
    //        when(sardine.list(base + "/abp/37/")) thenReturn abpE37VariantResources.asJava
    //        when(sardine.list(base + "/abp/38/")) thenReturn abpE38VariantResources.asJava
    //        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
    //        when(sardine.list(base + "/abp/40/")) thenReturn abpE40VariantResources.asJava
    //        when(sardine.list(base + "/abp/37/full/")) thenReturn file37Resources.asJava
    //        when(sardine.list(base + "/abp/38/full/")) thenReturn file38Resources.asJava
    //        when(sardine.list(base + "/abp/39W/full/")) thenReturn badFile39Resources.asJava
    //        when(sardine.list(base + "/abp/40/full/")) thenReturn file40Resources.asJava
    //        val finder = new WebdavFinder(logger, new SardineWrapper(logger, sardineFactory))
    //        // when
    //        val list = finder.findAvailable(new URL(base + "/"), "foo", "bar")
    //        // then
    //        val zip40_1 = WebDavFile(new URL(base + "/abp/40/full/DVD1.zip"), "DVD1.zip", false, false, true, Nil)
    //        val zip40_2 = WebDavFile(new URL(base + "/abp/40/full/DVD2.zip"), "DVD2.zip", false, false, true, Nil)
    //        list must be(List(
    //          OSGBProduct("abp", 40, List(abpZip40_1, abpZip40_2))
    //        ))
    //        // finally
    //      }
    //    }
  }

  //  "find latest available" should {
  //    """
  //      discover two products, each with their latest epoch
  //      and ignore any unimportant files
  //    """ in {
  //      new Context {
  //        // given
  //        when(sardine.list(base + "/")) thenReturn productResources.asJava
  //        when(sardine.list(base + "/abi/")) thenReturn abiEpochResources.asJava
  //        when(sardine.list(base + "/abi/40/")) thenReturn abiE40VariantResources.asJava
  //        when(sardine.list(base + "/abp/")) thenReturn abpEpochResources.asJava
  //        when(sardine.list(base + "/abp/37/")) thenReturn abpE37VariantResources.asJava
  //        when(sardine.list(base + "/abp/38/")) thenReturn abpE38VariantResources.asJava
  //        when(sardine.list(base + "/abp/39/")) thenReturn abpE39VariantResources.asJava
  //        when(sardine.list(base + "/abp/40/")) thenReturn abpE40VariantResources.asJava
  //        when(sardine.list(base + "/abp/37/full/")) thenReturn file37Resources.asJava
  //        when(sardine.list(base + "/abp/38/full/")) thenReturn file38Resources.asJava
  //        when(sardine.list(base + "/abp/39/full/")) thenReturn file39Resources.asJava
  //        when(sardine.list(base + "/abp/40/full/")) thenReturn file40Resources.asJava
  //        val finder = new WebdavFinder(logger, new SardineWrapper(logger, sardineFactory))
  //        // when
  //        val list = finder.findLatestAvailable(new URL(base + "/"), "foo", "bar")
  //        // then
  //        val zip40_1 = WebDavFile(new URL(base + "/abp/40/full/DVD1.zip"), "DVD1.zip", false, false, true, Nil)
  //        val zip40_2 = WebDavFile(new URL(base + "/abp/40/full/DVD2.zip"), "DVD2.zip", false, false, true, Nil)
  //        list must be(List(
  //          OSGBProduct("abp", 40, List(abpZip40_1, abpZip40_2))
  //        ))
  //        // finally
  //      }
  //    }
  //  }
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

