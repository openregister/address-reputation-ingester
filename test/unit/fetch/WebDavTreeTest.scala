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

  private def leafFile(product: String, epoch: Int, name: String) = {
    WebDavFile(new URL(s"$base/$product/$epoch/full/$name"), name, isZipFile = name.endsWith(".zip"), isPlainText = name.endsWith(".txt"))
  }

  "WebDavTree.name" should {
    "handle name with dot" in {
      leafFile("abp", 40, "DVD2.txt").name must be("DVD2")
    }

    "handle name without dot" in {
      val dir = WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true)
      dir.name must be("full")
    }
  }

  "find available" should {
    """
      discover one specified product with two epochs
      and ignore any unimportant files
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abi", 39, "DVD1.zip"),
                leafFile("abi", 39, "DVD1.txt")
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 39, "DVD1.zip"),
                leafFile("abp", 39, "DVD1.txt")
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 40, "DVD1.zip"),
                leafFile("abp", 40, "DVD1.txt"),
                leafFile("abp", 40, "DVD2.zip"),
                leafFile("abp", 40, "DVD2.txt")
              ))
            ))
          ))
        )))

      // when
      val list = tree.findAvailableFor("abp")

      // then
      list must be(List(
        OSGBProduct("abp", 39, List(leafFile("abp", 39, "DVD1.zip"))),
        OSGBProduct("abp", 40, List(leafFile("abp", 40, "DVD1.zip"), leafFile("abp", 40, "DVD2.zip")))
      ))
    }

    """
      discover one specified product with one specified epoch
      and ignore any unrelated files
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abi", 39, "DVD1.zip"),
                leafFile("abi", 39, "DVD1.txt")
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 39, "DVD1.zip"),
                leafFile("abp", 39, "DVD1.txt")
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 40, "DVD1.zip"),
                leafFile("abp", 40, "DVD1.txt"),
                leafFile("abp", 40, "DVD2.zip"),
                leafFile("abp", 40, "DVD2.txt")
              ))
            ))
          ))
        )))

      // when
      val product = tree.findAvailableFor("abp", "39")

      // then
      product must be(Some(OSGBProduct("abp", 39, List(leafFile("abp", 39, "DVD1.zip")))))
    }

    """
      discover nothing when the set is incomplete
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 40, "DVD1.zip"),
                leafFile("abp", 40, "DVD1.txt"),
                leafFile("abp", 40, "DVD2.zip")
              ))
            ))
          ))
        )))

      // when
      val list = tree.findAvailableFor("abp")

      // then
      list must be(Nil)
    }

    """
      discover one specified product with one specified epoch in subdirectories
      and ignore any unrelated files
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abi", 39, "data/001.zip"),
                leafFile("abi", 39, "data/001.txt")
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/data"), "data", isDirectory = true, files = List(
                  leafFile("abp", 39, "001.zip"),
                  leafFile("abp", 39, "001.txt")
                ))
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/40/full/data"), "data", isDirectory = true, files = List(
                  leafFile("abp", 40, "001.zip"),
                  leafFile("abp", 40, "001.txt"),
                  leafFile("abp", 40, "002.zip"),
                  leafFile("abp", 40, "002.txt"),
                  leafFile("abp", 40, "003.zip"),
                  leafFile("abp", 40, "003.txt")
                ))
              ))
            ))
          ))
        )))

      // when
      val list = tree.findAvailableFor("abp", "40")

      // then
      list must be(Some(OSGBProduct("abp", 40,
        List(leafFile("abp", 40, "001.zip"), leafFile("abp", 40, "002.zip"), leafFile("abp", 40, "003.zip")))))
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
                leafFile("abi", 39, "DVD1.zip"),
                leafFile("abi", 39, "DVD1.txt")
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 39, "DVD1.zip"),
                leafFile("abp", 39, "DVD1.txt")
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 40, "DVD1.zip"),
                leafFile("abp", 40, "DVD1.txt"),
                leafFile("abp", 40, "DVD2.zip"),
                leafFile("abp", 40, "DVD2.txt")
              ))
            ))
          ))
        )))

      // when
      val list = tree.findLatestFor("abp")

      // then
      list must be(Some(OSGBProduct("abp", 40, List(leafFile("abp", 40, "DVD1.zip"), leafFile("abp", 40, "DVD2.zip")))))
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

