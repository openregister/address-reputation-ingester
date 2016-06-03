/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package fetch

import java.io.File
import java.net.URL
import java.util.Date

import com.github.sardine.Sardine
import db.{CollectionMetadata, CollectionName, OutputDBWriterFactory}
import ingest.{StubContinuer, StubWorkerFactory}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.WorkQueue
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class FetchControllerTest extends PlaySpec with MockitoSugar {

  val base = "http://somedavserver.com:81/webdav"
  val url = new URL(base)
  val username = "foo"
  val password = "bar"
  val zip1 = WebDavFile(new URL(base + "/product/123/variant/DVD1.zip"), "DVD1.zip", isZipFile = true)
  val zip2 = WebDavFile(new URL(base + "/product/123/variant/DVD2.zip"), "DVD2.zip", isZipFile = true)

  val outputDirectory = new File(System.getProperty("java.io.tmpdir") + "/fetch-controller-test")
  val downloadDirectory = new File(outputDirectory, "downloads")
  val fooDirectory = new File(downloadDirectory, "foo")
  val barDirectory = new File(downloadDirectory, "bar")

  val anyDate = new Date(0)
  val settings = WriterSettings(1, 0)
  val continuer = new StubContinuer

  val foo_38_001 = CollectionName("foo_38_001").get
  val foo_39_001 = CollectionName("foo_39_001").get
  val foo_40_001 = CollectionName("foo_40_001").get
  val bar_40_002 = CollectionName("bar_40_002").get

  trait context {
    val logger = new StubLogger
    val status = new StatusLogger(logger)

    val sardine = mock[Sardine]
    val sardineWrapper = mock[SardineWrapper]
    when(sardineWrapper.begin) thenReturn sardine

    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)

    val webdavFetcher = mock[WebdavFetcher]
    val request = FakeRequest()
    val collectionMetadata = mock[CollectionMetadata]

    val ingesterFactory = mock[IngesterFactory]
    val dbWriterFactory = mock[OutputDBWriterFactory]

    val fetchController = new FetchController(status, workerFactory, webdavFetcher, ingesterFactory, dbWriterFactory, sardineWrapper, url, collectionMetadata)

    def parameterTest(product: String, epoch: Int, variant: String): Unit = {
      //      val writerFactory = mock[OutputFileWriterFactory]
      //      val request = FakeRequest()
      //
      //      intercept[IllegalArgumentException] {
      //        await(call(fetchController.doFetch(product, epoch, variant, None), request))
      //      }
    }

    def teardown() {
      worker.terminate()
      Utils.deleteDir(outputDirectory)
    }
  }


  "parameter test" should {
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """ in {
      new context {
        parameterTest("$%", 40, "full")
      }
    }

    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """ in {
      new context {
        parameterTest("abi", 40, ")(")
      }
    }
  }


  "doFetch" should {
    "use the work queue to download files via webdav" in {
      new context {
        // given
        val tree = WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/product/"), "product", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/product/123/"), "123", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/product/123/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/product/123/full/DVD1.zip"), "DVD1.zip", isZipFile = true),
                  WebDavFile(new URL(base + "/product/123/full/DVD1.txt"), "DVD1.txt", isPlainText = true)
                ))
              ))
            ))
          )))
        when(sardineWrapper.exploreRemoteTree) thenReturn tree

        val f1Txt = new DownloadedFile("/a/b/DVD1.txt")
        val f1Zip = new DownloadedFile("/a/b/DVD1.zip")
        val f2Txt = new DownloadedFile("/a/b/DVD2.txt")
        val f2Zip = new DownloadedFile("/a/b/DVD2.zip")
        val files = List(f1Txt, f1Zip, f2Txt, f2Zip)
        val items = List(DownloadItem.fresh(f1Txt), DownloadItem.fresh(f1Zip), DownloadItem.fresh(f2Txt), DownloadItem.fresh(f2Zip))
        when(webdavFetcher.fetchList(any[OSGBProduct], anyString, any[Boolean])) thenReturn items

        // when
        val response = await(call(fetchController.doFetch("product", 123, "variant", None, None, Some(true)), request))

        // then
        worker.awaitCompletion()
        assert(response.header.status === 202)
        verify(sardineWrapper).exploreRemoteTree
        verify(webdavFetcher).fetchList(any[OSGBProduct], anyString, any[Boolean])
//        assert(logger.infos.map(_.message) === List(
//          "Info:Starting fetching product/123/variant.",
//          "Info:Finished fetching product/123/variant after {}."
//        ))
//        assert(logger.size === 2)
        teardown()
      }
    }
  }


  "file fetching" should {
    "download files using webdav then unzip every zip file" in {
      new context {
        // given
        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product))

        val f1Txt = new DownloadedFile("/a/b/f1.txt")
        val f1Zip = new DownloadedFile("/a/b/f1.zip")
        val f2Txt = new DownloadedFile("/a/b/f2.txt")
        val f2Zip = new DownloadedFile("/a/b/f2.zip")
        val files = List(f1Txt, f1Zip, f2Txt, f2Zip)
        val items = List(DownloadItem.fresh(f1Txt), DownloadItem.fresh(f1Zip), DownloadItem.fresh(f2Txt), DownloadItem.fresh(f2Zip))
        when(webdavFetcher.fetchList(any[OSGBProduct], anyString, any[Boolean])) thenReturn items

        // when
//        val model2 = fetchController.fetch(model1, settings, continuer)
//
//        // then
//        assert(model2 === model1)
//        verify(webdavFetcher).fetchList(any[OSGBProduct], anyString, any[Boolean])
//        assert(logger.size === 0)
        teardown()
      }
    }

    "download files using webdav but only unzip fresh zip files" in {
      new context {
        // given
        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product))

        val f1Txt = new DownloadedFile("/a/b/f1.txt")
        val f1Zip = new DownloadedFile("/a/b/f1.zip")
        val f2Txt = new DownloadedFile("/a/b/f2.txt")
        val f2Zip = new DownloadedFile("/a/b/f2.zip")
        val items = List(DownloadItem.stale(f1Txt), DownloadItem.stale(f1Zip), DownloadItem.fresh(f2Txt), DownloadItem.fresh(f2Zip))
        when(webdavFetcher.fetchList(any[OSGBProduct], anyString, any[Boolean])) thenReturn items

        // when
        val model2 = fetchController.fetch(model1, settings, continuer)

        // then
        assert(model2 === model1)
        verify(webdavFetcher).fetchList(any[OSGBProduct], anyString, any[Boolean])
        assert(logger.size === 0)
        teardown()
      }
    }

    """given a list of not-yet-downloaded files passed in via the model,
       fetch should download the files
       then unzip all of them
    """ in {
      new context {
        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product))

        val f1Txt = new DownloadedFile("/a/b/product/123/variant/DVD1.txt")
        val f1Zip = new DownloadedFile("/a/b/product/123/variant/DVD1.zip")
        val items = List(DownloadItem.fresh(f1Txt), DownloadItem.fresh(f1Zip))
        when(webdavFetcher.fetchList(product, "product/123/variant", false)) thenReturn items

        // when
        val model2 = fetchController.fetch(model1, settings, continuer)

        // then
        assert(model2 === model1)
        verify(webdavFetcher).fetchList(product, "product/123/variant", false)
        assert(logger.size === 0)
        teardown()
      }
    }

    """given a list of pre-existing (i.e. stale) files passed in via the model,
       fetch should not download the files
    """ in {
      new context {
        // given
        val tree = WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/product/"), "product", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/product/123/"), "123", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/product/123/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/product/123/full/DVD1.zip"), "DVD1.zip", isZipFile = true),
                  WebDavFile(new URL(base + "/product/123/full/DVD1.txt"), "DVD1.txt", isPlainText = true)
                ))
              ))
            ))
          )))
        when(sardineWrapper.exploreRemoteTree) thenReturn tree

        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product))

        val f1Txt = new DownloadedFile("/a/b/DVD1.txt")
        val f1Zip = new DownloadedFile("/a/b/DVD1.zip")
        val items = List(DownloadItem.stale(f1Txt), DownloadItem.stale(f1Zip))
        when(webdavFetcher.fetchList(product, "product/123/variant", false)) thenReturn items

        // when
        val model2 = fetchController.fetch(model1, settings, continuer)

        // then
        assert(model2 === model1)
        verify(webdavFetcher).fetchList(product, "product/123/variant", false)
        assert(logger.size === 0)
        teardown()
      }
    }

    """given a list of pre-existing (i.e. stale) files passed in via the model,
       when forceFetch is set,
       fetch should download the files
    """ in {
      new context {
        // given
        val tree = WebDavTree(
          WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/product/"), "product", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/product/123/"), "123", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/product/123/full/"), "full", isDirectory = true, files = List(
                  WebDavFile(new URL(base + "/product/123/full/DVD1.zip"), "DVD1.zip", isZipFile = true),
                  WebDavFile(new URL(base + "/product/123/full/DVD1.txt"), "DVD1.txt", isPlainText = true)
                ))
              ))
            ))
          )))
        when(sardineWrapper.exploreRemoteTree) thenReturn tree

        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product), forceChange = true)

        val f1Txt = new DownloadedFile("/a/b/DVD1.txt")
        val f1Zip = new DownloadedFile("/a/b/DVD1.zip")
        val items = List(DownloadItem.fresh(f1Txt), DownloadItem.fresh(f1Zip))
        when(webdavFetcher.fetchList(product, "product/123/variant", true)) thenReturn items

        // when
        val model2 = fetchController.fetch(model1, settings, continuer)

        // then
        assert(model2.hasFailed === false)
        verify(webdavFetcher).fetchList(product, "product/123/variant", true)
        assert(logger.size === 0)
        teardown()
      }
    }

    "when passed an empty file list, fetch should leave the model in a failed state" in {
      new context {
        // given
        val product = OSGBProduct("product", 123, List(zip1))
        val model1 = StateModel("product", 123, Some("variant"), None, Some(product))

        val items = List[DownloadItem]()
        when(webdavFetcher.fetchList(product, "product/123/variant", false)) thenReturn items

        // when
        val model2 = fetchController.fetch(model1, settings, continuer)

        // then
        assert(model2 === model1.copy(hasFailed = true))
        verify(webdavFetcher).fetchList(product, "product/123/variant", false)
        assert(logger.size === 0)
        teardown()
      }
    }
  }
}
