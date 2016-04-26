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

import java.io.{File, FileInputStream}
import java.net.{URI, URL}
import java.nio.file.{Files, Path}

import com.github.sardine.{DavResource, Sardine}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito
import uk.co.hmrc.logging.StubLogger

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class WebdavFetcherTest extends PlaySpec with Mockito {

  class Context(resourceFolder: String) {
    val logger = new StubLogger()
    val outputDirectory = Files.createTempDirectory("webdav-fetcher-test")
    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineFactory2]
    when(sardineFactory.begin("user", "pass")) thenReturn sardine

    val url = "http://somedavserver.com/path/prod/rel/variant/"
    val files = new File(getClass.getResource(resourceFolder).toURI).listFiles().toList
    val resources = files.map(toDavResource(_, url))

    def teardown(): Unit = {
      deleteDir(outputDirectory)
    }

    private def deleteDir(path: Path): Unit = {
      path.toFile.listFiles().to[Seq].foreach(f => f.delete())
      path.toFile.delete()
    }
  }

  "fetch all" should {
    "copy files to output directory" in new Context("/webdav") {
      // given
      when(sardine.list(url)) thenReturn resources
      files.foreach {
        f =>
          val s = s"$url${f.getName}"
          when(sardine.get(s)) thenReturn new FileInputStream(f)
      }
      val fetcher = new WebdavFetcher(logger, sardineFactory)
      // when
      val total = fetcher.fetchAll(url, "user", "pass", outputDirectory)
      // then
      total must be(files.map(_.length()).sum)
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      outputDirectory.toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message).toSet must be(Set(
        "Info:Fetched foo.txt",
        "Info:Fetched bar.txt",
        "Info:Fetched baz.txt"
      ))
      // finally
      teardown()
    }
  }

  "fetch list" should {
    "copy files to output directory" in new Context("/webdav") {
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
      val fetcher = new WebdavFetcher(logger, sardineFactory)
      // when
      val total = fetcher.fetchList(product, "user", "pass", outputDirectory)
      // then
      total must be(files.map(_.length()).sum)
      val doneFiles: Set[String] = files.map(_.getName + ".done").toSet
      outputDirectory.toFile.list().toSet must be(files.map(_.getName).toSet ++ doneFiles)
      logger.infos.map(_.message).toSet must be(Set(
        "Info:Fetched foo.txt",
        "Info:Fetched bar.txt",
        "Info:Fetched baz.txt"
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
