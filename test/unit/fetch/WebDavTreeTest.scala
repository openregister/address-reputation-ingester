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

  private def leafFile(product: String, epoch: Int, name: String, kb: Long = 0L) = {
    WebDavFile(new URL(s"$base/$product/$epoch/full/$name"), name, kb = kb,
      isDataFile = name.endsWith(".zip") || name.endsWith(".csv"),
      isPlainText = name.endsWith(".txt"))
  }

  //  "fix app.conf" should { "" in { fail() }}

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
                leafFile("abp", 39, "DVD1.txt"),
                leafFile("abp", 39, "ignore.this")
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
                leafFile("abp", 40, "DVD2.zip") // missing txt
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
                  leafFile("abp", 40, "003.txt"),
                  leafFile("abp", 40, "ignore.this")
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
                leafFile("abp", 39, "DVD1.csv"),
                leafFile("abp", 39, "DVD1.txt")
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 40, "DVD1.csv"),
                leafFile("abp", 40, "DVD1.txt"),
                leafFile("abp", 40, "DVD2.csv"),
                leafFile("abp", 40, "DVD2.txt"),
                leafFile("abp", 40, "ignore.this")
              ))
            ))
          ))
        )))

      // when
      val list = tree.findLatestFor("abp")

      // then
      list must be(Some(OSGBProduct("abp", 40, List(leafFile("abp", 40, "DVD1.csv"), leafFile("abp", 40, "DVD2.csv")))))
    }
  }

  "toString" should {
    """
      produce helpful listings of files and directories
    """ in {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abi", 39, "DVD1.zip", 123456L),
                leafFile("abi", 39, "DVD1.txt", 1L)
              ))
            ))
          )),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                leafFile("abp", 39, "DVD1.csv", 999L),
                leafFile("abp", 39, "DVD1.txt", 0L)
              ))
            )),
            WebDavFile(new URL(base + "/abp/40/"), "40", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/40/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/40/full/data/"), "data", isDirectory = true, files = List(
                  leafFile("abp", 40, "AddressBasePremium_FULL_2016-05-18_001_csv.zip", 8877L),
                  leafFile("abp", 40, "AddressBasePremium_FULL_2016-05-18_001_csv.txt"),
                  leafFile("abp", 40, "AddressBasePremium_FULL_2016-05-18_002_csv.zip", 9988L),
                  leafFile("abp", 40, "AddressBasePremium_FULL_2016-05-18_002_csv.txt"),
                  leafFile("abp", 40, "ignore.this", 4321L)
                ))
              ))
            ))
          ))
        )))

      // when
      val info = tree.toString
//      println(info)

      // then
      info must be(
        """WebDavTree(webdav/
          |  abi/
          |    39/
          |      full/
          |        DVD1.zip                                           (data)     123456 KiB
          |        DVD1.txt                                           (txt)           1 KiB
          |  abp/
          |    39/
          |      full/
          |        DVD1.csv                                           (data)        999 KiB
          |        DVD1.txt                                           (txt)           0 KiB
          |    40/
          |      full/
          |        data/
          |          AddressBasePremium_FULL_2016-05-18_001_csv.zip     (data)       8877 KiB
          |          AddressBasePremium_FULL_2016-05-18_001_csv.txt     (txt)           0 KiB
          |          AddressBasePremium_FULL_2016-05-18_002_csv.zip     (data)       9988 KiB
          |          AddressBasePremium_FULL_2016-05-18_002_csv.txt     (txt)           0 KiB
          |          ignore.this                                                     4321 KiB
          |)""".stripMargin)
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

