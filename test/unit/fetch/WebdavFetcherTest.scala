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

import java.io.{File, FileInputStream}
import java.net.{URI, URL}

import com.github.sardine.{DavResource, Sardine}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito
import Utils._
import ingest.{StubContinuer, StubFileProcessor}
import services.model.StatusLogger
import uk.co.hmrc.logging.StubLogger

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class WebdavFetcherTest extends PlaySpec with Mockito {

  val outputDirectory = new File(System.getProperty("java.io.tmpdir") + "/webdav-fetcher-test")
  val downloadDirectory = new File(outputDirectory, "downloads")
  val stuff = new File(downloadDirectory, "stuff")

  class Context(resourceFolder: String) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineWrapper]
    when(sardineFactory.begin) thenReturn sardine

    val url = "http://somedavserver.com/path/prod/rel/variant/"
    val files = new File(getClass.getResource(resourceFolder).toURI).listFiles().toList.sorted
    val resources = files.map(toDavResource(_, url))

    deleteDir(outputDirectory)
    stuff.mkdirs()

    def teardown() {
      deleteDir(outputDirectory)
    }
  }

//  "fetch all" should {
//    "copy files to output directory if it is empty" in new Context("/webdav") {
//      // given
//      when(sardine.list(url)) thenReturn resources
//      files.foreach {
//        f =>
//          val s = s"$url${f.getName}"
//          when(sardine.get(s)) thenReturn new FileInputStream(f)
//      }
//      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)
//      // when
//      val downloaded = fetcher.fetchAll(url, "stuff", false)
//      // then
//      downloaded.size must be(3)
//      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
//      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
//      stuff.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
//      logger.infos.map(_.message) must be(List(
//        "Info:Listing {}.",
//        "Info:Fetching {} to bar.txt.",
//        "Info:Fetched bar.txt in {}.",
//        "Info:Fetching {} to baz.txt.",
//        "Info:Fetched baz.txt in {}.",
//        "Info:Fetching {} to foo.txt.",
//        "Info:Fetched foo.txt in {}."
//      ))
//      // finally
//      teardown()
//    }
//
//    "copy files to output directory if the files are already present but without .done markers" in new Context("/webdav") {
//      // given
//      when(sardine.list(url)) thenReturn resources
//      files.foreach {
//        f =>
//          val s = s"$url${f.getName}"
//          when(sardine.get(s)) thenReturn new FileInputStream(f)
//          createSomeFile(stuff, f.getName)
//      }
//      Thread.sleep(10)
//      // because the filesystem can be slightly behind
//      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)
//      // when
//      val downloaded = fetcher.fetchAll(url, "stuff", false)
//      // then
//      downloaded.size must be(3)
//      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
//      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
//      stuff.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
//      logger.infos.map(_.message) must be(List(
//        "Info:Listing {}.",
//        "Info:Fetching {} to bar.txt.",
//        "Info:Fetched bar.txt in {}.",
//        "Info:Fetching {} to baz.txt.",
//        "Info:Fetched baz.txt in {}.",
//        "Info:Fetching {} to foo.txt.",
//        "Info:Fetched foo.txt in {}."
//      ))
//      // finally
//      teardown()
//    }
//
//    "copy files to output directory if the files are already present but with older .done markers" in new Context("/webdav") {
//      // given
//      when(sardine.list(url)) thenReturn resources
//      files.foreach {
//        f =>
//          val s = s"$url${f.getName}"
//          when(sardine.get(s)) thenReturn new FileInputStream(f)
//          createSomeFile(stuff, f.getName, 2000, "")
//          createSomeFile(stuff, f.getName, 4000, ".done")
//      }
//      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)
//
//      // when
//      val downloaded = fetcher.fetchAll(url, "stuff", false)
//
//      // then
//      downloaded.size must be(3)
//      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
//      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
//      stuff.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
//      logger.infos.map(_.message) must be(List(
//        "Info:Listing {}.",
//        "Info:Fetching {} to bar.txt.",
//        "Info:Fetched bar.txt in {}.",
//        "Info:Fetching {} to baz.txt.",
//        "Info:Fetched baz.txt in {}.",
//        "Info:Fetching {} to foo.txt.",
//        "Info:Fetched foo.txt in {}."
//      ))
//      // finally
//      teardown()
//    }
//
//    "not copy files to output directory if the files are already present and with younger .done markers" in new Context("/webdav") {
//      // given
//      when(sardine.list(url)) thenReturn resources
//      files.foreach {
//        f =>
//          val s = s"$url${f.getName}"
//          when(sardine.get(s)) thenReturn new FileInputStream(f)
//          createSomeFile(stuff, f.getName, 4000, "")
//          createSomeFile(stuff, f.getName, 2000, ".done")
//      }
//      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)
//
//      // when
//      val downloaded = fetcher.fetchAll(url, "stuff", false)
//
//      // then
//      downloaded.size must be(3)
//      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
//      stuff.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
//      logger.infos.map(_.message) must be(List(
//        "Info:Listing {}.",
//        "Info:Already had bar.txt.",
//        "Info:Already had baz.txt.",
//        "Info:Already had foo.txt."
//      ))
//      // finally
//      teardown()
//    }
//
//    "copy files to output directory when forced, even if the files are already present and with younger .done markers" in new Context("/webdav") {
//      // given
//      when(sardine.list(url)) thenReturn resources
//      files.foreach {
//        f =>
//          val s = s"$url${f.getName}"
//          when(sardine.get(s)) thenReturn new FileInputStream(f)
//          createSomeFile(stuff, f.getName, 4000, "")
//          createSomeFile(stuff, f.getName, 2000, ".done")
//      }
//      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)
//
//      // when
//      val downloaded = fetcher.fetchAll(url, "stuff", true)
//
//      // then
//      downloaded.size must be(3)
//      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
//      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
//      stuff.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
//      logger.infos.map(_.message) must be(List(
//        "Info:Listing {}.",
//        "Info:Fetching {} to bar.txt (forced).",
//        "Info:Fetched bar.txt in {}.",
//        "Info:Fetching {} to baz.txt (forced).",
//        "Info:Fetched baz.txt in {}.",
//        "Info:Fetching {} to foo.txt (forced).",
//        "Info:Fetched foo.txt in {}."
//      ))
//      // finally
//      teardown()
//    }
//  }

  "fetch list" should {
    "copy files to output directory if it is empty" in new Context("/webdav") {
      // given
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
      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)

      // when
      val downloaded = fetcher.fetchList(product, "stuff", false, new StubContinuer, new StubFileProcessor)

      // then
      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
      downloadDirectory.toPath.resolve("stuff").toFile.list().toSet must be(files.map(_.getName).toSet)
      logger.infos.map(_.message) must be(List(
        "Info:Fetching {} to bar.txt.",
        "Info:Fetched bar.txt in {}.",
        "Info:Fetched and processed bar.txt in {}.",
        "Info:Fetching {} to baz.txt.",
        "Info:Fetched baz.txt in {}.",
        "Info:Fetched and processed baz.txt in {}.",
        "Info:Fetching {} to foo.txt.",
        "Info:Fetched foo.txt in {}.",
        "Info:Fetched and processed foo.txt in {}."
      ))
      // finally
      teardown()
    }

    "not copy files to output directory if the target files are present and fresh" ignore new Context("/webdav") {
      // given
      when(sardine.list(url)) thenReturn resources
      files.foreach {
        f =>
          val s = s"$url${f.getName}"
          when(sardine.get(s)) thenReturn new FileInputStream(f)
          createSomeFile(stuff, f.getName, 4000, "")
          createSomeFile(stuff, f.getName, 2000, ".done")
      }
      val webDavFiles = files.map {
        f =>
          WebDavFile(new URL(s"$url${f.getName}"), f.getName, false, false, true, Nil)
      }
      val product = OSGBProduct("abp", 39, webDavFiles)
      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)

      // when
      val downloaded = fetcher.fetchList(product, "stuff", false, new StubContinuer, new StubFileProcessor)

      // then
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      downloadDirectory.toPath.resolve("stuff").toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message) must be(List(
        "Info:Already had bar.txt.",
        "Info:Already had baz.txt.",
        "Info:Already had foo.txt."
      ))
      // finally
      teardown()
    }

    "copy files to output directory if forced" in new Context("/webdav") {
      // given
      when(sardine.list(url)) thenReturn resources
      files.foreach {
        f =>
          val s = s"$url${f.getName}"
          when(sardine.get(s)) thenReturn new FileInputStream(f)
          createSomeFile(stuff, f.getName, 4000, "")
          createSomeFile(stuff, f.getName, 2000, ".done")
      }
      val webDavFiles = files.map {
        f =>
          WebDavFile(new URL(s"$url${f.getName}"), f.getName, false, false, true, Nil)
      }
      val product = OSGBProduct("abp", 39, webDavFiles)
      val fetcher = new WebdavFetcher(sardineFactory, downloadDirectory, status)

      // when
      val downloaded = fetcher.fetchList(product, "stuff", true, new StubContinuer, new StubFileProcessor)

      // then
      downloaded.map(_.file.length).sum must be(files.map(_.length()).sum)
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      downloadDirectory.toPath.resolve("stuff").toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message) must be(List(
        "Info:Fetching {} to bar.txt (forced).",
        "Info:Fetched bar.txt in {}.",
        "Info:Fetched and processed bar.txt in {}.",
        "Info:Fetching {} to baz.txt (forced).",
        "Info:Fetched baz.txt in {}.",
        "Info:Fetched and processed baz.txt in {}.",
        "Info:Fetching {} to foo.txt (forced).",
        "Info:Fetched foo.txt in {}.",
        "Info:Fetched and processed foo.txt in {}."
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

  private def createSomeFile(dir: File, name: String, age: Long = 0, suffix: String = "") {
    val file = new File(dir, name + suffix)
    file.createNewFile()
    if (age > 0) {
      file.setLastModified(System.currentTimeMillis - age)
    }
  }
}
