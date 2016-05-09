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

import java.io.{File, FileInputStream}
import java.net.{URI, URL}

import com.github.sardine.{DavResource, Sardine}
import config.Utils._
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito
import uk.co.hmrc.logging.StubLogger

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class WebdavFetcherTest extends PlaySpec with Mockito {

  val outputDirectory = new File(System.getProperty("java.io.tmpdir") + "/webdav-fetcher-test")

  class Context(resourceFolder: String) {
    val logger = new StubLogger()
    val downloadDirectory = new File(outputDirectory, "downloads")
    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineWrapper]
    when(sardineFactory.begin) thenReturn sardine

    val url = "http://somedavserver.com/path/prod/rel/variant/"
    val files = new File(getClass.getResource(resourceFolder).toURI).listFiles().toList
    val resources = files.map(toDavResource(_, url))

    def teardown() {
      deleteDir(outputDirectory)
    }
  }

  "fetch all" should {
    "copy files to output directory if it is empty" in new Context("/webdav") {
      // given
      deleteDir(outputDirectory)
      when(sardine.list(url)) thenReturn resources
      files.foreach {
        f =>
          val s = s"$url${f.getName}"
          when(sardine.get(s)) thenReturn new FileInputStream(f)
      }
      val fetcher = new WebdavFetcher(logger, sardineFactory, downloadDirectory)
      // when
      val downloaded = fetcher.fetchAll(url, "stuff")
      // then
      downloaded.map(_.length()).sum must be(files.map(_.length()).sum)
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      downloadDirectory.toPath.resolve("stuff").toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message).sorted must be(List(
        "Info:Fetched bar.txt in {}",
        "Info:Fetched baz.txt in {}",
        "Info:Fetched foo.txt in {}",
        "Info:Fetching {} to bar.txt",
        "Info:Fetching {} to baz.txt",
        "Info:Fetching {} to foo.txt",
        "Info:Listing {}"
      ))
      // finally
      teardown()
    }

    "not copy files to output directory if the files are already preseny" in new Context("/webdav") {
      //TODO
    }
  }

  "fetch list" should {
    "copy files to output directory if it is empty" in new Context("/webdav") {
      // given
      deleteDir(outputDirectory)
      when(sardine.list(url)) thenReturn resources
      files.foreach {
        f =>
          val s = s"$url${f.getName}"
          when(sardine.get(s)) thenReturn new FileInputStream(f)
      }
      val webDavFiles = files.map {
        f =>
          WebDavFile(new URL(s"$url${f.getName}"), f.getName, false, false, true, Nil)
      }
      val product = OSGBProduct("abp", 39, webDavFiles)
      val fetcher = new WebdavFetcher(logger, sardineFactory, downloadDirectory)
      // when
      val downloaded = fetcher.fetchList(product, "stuff")
      // then
      downloaded.map(_.length()).sum must be(files.map(_.length()).sum)
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      downloadDirectory.toPath.resolve("stuff").toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message).sorted must be(List(
        "Info:Fetched bar.txt in {}",
        "Info:Fetched baz.txt in {}",
        "Info:Fetched foo.txt in {}",
        "Info:Fetching {} to bar.txt",
        "Info:Fetching {} to baz.txt",
        "Info:Fetching {} to foo.txt"
      ))
      // finally
      teardown()
    }
  }

  private def toDavResource(f: File, url: String): DavResource = {
    val res = mock[DavResource]
    when(res.getHref) thenReturn toUri(url, f)
    when(res.getName) thenReturn f.getName
    res
  }

  private def toUri(url: String, f: File) = {
    val u = new URL(url)
    val s = s"${u.getPath}${f.getName}"
    new URI(s)
  }
}
