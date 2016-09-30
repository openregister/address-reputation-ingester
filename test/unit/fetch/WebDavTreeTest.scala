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

  import WebDavTree.readyToCollectFile

  val base = "http://somedavserver.com:81"

  private def folder(path: String, name: String, files: List[WebDavFile]) = {
    WebDavFile(new URL(s"$base/$path$name"), name, kb = 0L, files = files,
      isDirectory = true,
      isDataFile = false,
      isPlainText = false)
  }

  private def leafFile(path: String, name: String, kb: Long = 0L) = {
    WebDavFile(new URL(s"$base/$path$name"), name, kb = kb,
      isDataFile = name.endsWith(".zip") || name.endsWith(".csv"),
      isPlainText = name.endsWith(".txt"))
  }

  //  "fix app.conf" should { "" ignore { fail() }}

  "WebDavTree.name" should {
    "handle name with dot" in {
      leafFile("webdav/abp/40", "DVD2.txt").name must be("DVD2")
    }

    "handle name without dot" in {
      val dir = folder("abi/39", "full", Nil)
      dir.name must be("full")
    }
  }

  "find available" should {
    """
      discover one specified product with two epochs
      and ignore any unimportant files
      and ignore products except those asked for
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abi", List(
            folder("webdav/abi/", "39", List(
              folder("webdav/abi/39/", "full", List(
                leafFile("webdav/abi/39/full/", "x1.zip"),
                leafFile("webdav/abi/39/full/", readyToCollectFile)
              ))
            ))
          )),
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                leafFile("webdav/abp/39/full/", "abp1.zip"),
                leafFile("webdav/abp/39/full/", readyToCollectFile)
              ))
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                folder("webdav/abp/40/full/", "data", List(
                  leafFile("webdav/abp/40/full/data/", "DVD1.zip"),
                  leafFile("webdav/abp/40/full/data/", "DVD2.zip")
                )),
                leafFile("webdav/abp/40/full/", readyToCollectFile)
              ))
            )),
            folder("webdav/abp/", "41", List(
              folder("webdav/abp/41/", "full", List(
                folder("webdav/abp/41/full/", "data", List(
                  leafFile("webdav/abp/41/full/data/", "file1.zip"),
                  leafFile("webdav/abp/41/full/data/", "file2.zip"),
                  leafFile("webdav/abp/41/full/data/", readyToCollectFile)
                ))
              ))
            ))
          ))
        ))
      )

      // when
      val list = tree.findAvailableFor("abp")

      // then
      list must be(List(
        OSGBProduct("abp", 39, List(leafFile("webdav/abp/39/full/", "abp1.zip"))),
        OSGBProduct("abp", 40, List(leafFile("webdav/abp/40/full/data/", "DVD1.zip"), leafFile("webdav/abp/40/full/data/", "DVD2.zip"))),
        OSGBProduct("abp", 41, List(leafFile("webdav/abp/41/full/data/", "file1.zip"), leafFile("webdav/abp/41/full/data/", "file2.zip")))
      ))
    }

    """
      discover one specified product with one specified epoch
      and ignore any unrelated files
      and ignore products except those asked for
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abi", List(
            folder("webdav/abi/", "39", List(
              folder("webdav/abi/39/", "full", List(
                leafFile("webdav/abi/39/full/", "DVD1.zip")
              ))
            ))
          )),
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                leafFile("webdav/abp/39/full/", "DVD1.zip"),
                leafFile("webdav/abp/39/full", readyToCollectFile),
                leafFile("webdav/abp/39/full/", "ignore.this")
              ))
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                leafFile("webdav/abp/40/full/", "DVD1.zip"),
                leafFile("webdav/abp/40/full/", "DVD2.zip"),
                leafFile("webdav/abp/40/full", readyToCollectFile)
              ))
            ))
          ))
        )))

      // when
      val product = tree.findAvailableFor("abp", 39)

      // then
      product must be(Some(OSGBProduct("abp", 39, List(leafFile("webdav/abp/39/full/", "DVD1.zip")))))
    }

    """
      discover nothing when the set is incomplete
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                leafFile("webdav/abp/40/full/", "DVD1.zip"),
                leafFile("webdav/abp/40/full/", "DVD2.zip") // missing ready-to-collect.txt
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
      discover two products with one epoch in subdirectories
      and ignore any unrelated files
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abi", List(
            folder("webdav/abi/", "39", List(
              folder("webdav/abi/39/", "full", List(
                folder("webdav/abp/39/full/", "data", List(
                  leafFile("webdav/abi/39/full/data/", "001.zip")
                ))
              )),
              leafFile("webdav/abi/39/full/", readyToCollectFile)
            ))
          )),
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                folder("webdav/abp/39/full/", "data", List(
                  leafFile("webdav/abp/39/full/data/", "001.zip")
                ))
              )),
              leafFile("webdav/abp/39/", readyToCollectFile) // not in the right place
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                folder("webdav/abp/40/full/", "data", List(
                  leafFile("webdav/abp/40/full/data/", "001.zip"),
                  leafFile("webdav/abp/40/full/data/", "002.zip"),
                  leafFile("webdav/abp/40/full/data/", "003.zip"),
                  leafFile("webdav/abp/40/full/data/", "ignore.this")
                )),
                leafFile("webdav/abp/40/full", readyToCollectFile) // correct
              ))
            ))
          ))
        )))

      // when
      val list = tree.findAvailableFor("abp")

      // then
      list must be(List(
        OSGBProduct("abp", 40,
          List(leafFile("webdav/abp/40/full/data/", "001.zip"), leafFile("webdav/abp/40/full/data/", "002.zip"), leafFile("webdav/abp/40/full/data/", "003.zip")))))
    }
  }

  "find latest" should {
    """
      discover one product with one epoch where all products have corresponding ready-to-collect marker
      and ignore any unimportant files
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abi", List(
            folder("webdav/abi/", "39", List(
              folder("webdav/abi/39/", "full", List(
                leafFile("webdav/abi/39/full/", "DVD1.zip"),
                leafFile("webdav/abi/39/full/", readyToCollectFile)
              ))
            ))
          )),
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                leafFile("webdav/abp/39/full/", "DVD1.csv"),
                leafFile("webdav/abp/39/full", readyToCollectFile)
              ))
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                leafFile("webdav/abp/40/full/", "DVD1.csv"),
                leafFile("webdav/abp/40/full/", "DVD2.csv"),
                leafFile("webdav/abp/40/full/", readyToCollectFile),
                leafFile("webdav/abp/40/full/", "ignore.this")
              ))
            ))
          ))
        )))

      // when
      val list = tree.findLatestFor("abp")

      // then
      list must be(Some(OSGBProduct("abp", 40, List(leafFile("webdav/abp/40/full/", "DVD1.csv"), leafFile("webdav/abp/40/full/", "DVD2.csv")))))
    }

    """
      discover one product with one epoch where a ready-to-collect.txt marker exists
      but there are no other txt marker files
      and ignore any unimportant files
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                leafFile("webdav/abp/39/full/", "DVD1.zip"),
                leafFile("webdav/abp/39/full/", readyToCollectFile)
              ))
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                folder("webdav/abp/40/full/", "data", List(
                  leafFile("webdav/abp/40/full/data/", "ABP_2016-05-18_001_csv.zip"),
                  leafFile("webdav/abp/40/full/data/", "ABP_2016-05-18_002_csv.zip"),
                  leafFile("webdav/abp/40/full/data/", "ignore.this")
                )),
                leafFile("webdav/abp/40/full/", readyToCollectFile)
              ))
            ))
          ))
        )))

      // when
      val list = tree.findLatestFor("abp")

      // then
      list must be(Some(OSGBProduct("abp", 40,
        List(leafFile("webdav/abp/40/full/data/", "ABP_2016-05-18_001_csv.zip"), leafFile("webdav/abp/40/full/data/", "ABP_2016-05-18_002_csv.zip")))))
    }
  }

  "toString" should {
    """
      produce helpful listings of files and directories
    """ in {
      // given
      val tree = WebDavTree(
        folder("/", "webdav", List(
          folder("webdav/", "abi", List(
            folder("webdav/abi/", "39", List(
              folder("webdav/abi/39/", "full", List(
                leafFile("webdav/abi/39/full/", "DVD1.zip", 123456L),
                leafFile("webdav/abi/39/full/", "DVD1.txt", 1L)
              ))
            ))
          )),
          folder("webdav/", "abp", List(
            folder("webdav/abp/", "39", List(
              folder("webdav/abp/39/", "full", List(
                leafFile("webdav/abp/39/full/", "DVD1.csv", 999L),
                leafFile("webdav/abp/39/full/", "DVD1.txt", 0L)
              ))
            )),
            folder("webdav/abp/", "40", List(
              folder("webdav/abp/40/", "full", List(
                folder("webdav/abp/40/full/", "data", List(
                  leafFile("webdav/abp/40/full/data/", "AddressBasePremium_FULL_2016-05-18_001_csv.zip", 8877L),
                  leafFile("webdav/abp/40/full/data/", "AddressBasePremium_FULL_2016-05-18_001_csv.txt"),
                  leafFile("webdav/abp/40/full/data/", "AddressBasePremium_FULL_2016-05-18_002_csv.zip", 9988L),
                  leafFile("webdav/abp/40/full/data/", "AddressBasePremium_FULL_2016-05-18_002_csv.txt"),
                  leafFile("webdav/abp/40/full/data/", "ignore.this", 4321L)
                ))
              ))
            ))
          ))
        )))

      // when
      val info = tree.indentedString

      // then
      info must be(
        """webdav/
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
          |""".stripMargin)
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

