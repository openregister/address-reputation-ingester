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

package controllers

import java.io.File

import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.ingester.exec.{WorkQueue, WorkerFactory}
import services.ingester.fetch.{WebdavFetcher, ZipUnpacker}
import uk.co.hmrc.logging.StubLogger

class FetchControllerTest extends FunSuite with MockitoSugar {

  trait context {
    val testWorker = new WorkQueue(new StubLogger())
    val workerFactory = new WorkerFactory {
      override def worker = testWorker
    }
    val webdavFetcher = mock[WebdavFetcher]
    val unzipper = mock[ZipUnpacker]
    val logger = new StubLogger
    val url = "http://localhost/webdav"
    val username = "foo"
    val password = "bar"
    val controller = new FetchController(workerFactory, logger, webdavFetcher, unzipper, url, username, password)
    val req = FakeRequest()

    def teardown() {
      testWorker.terminate()
    }
  }


  test("fetch should download files using webdav then unzip every zip file") {
    new context {
      val product = "product"
      val epoch = 123
      val variant = "variant"
      val f1Txt = new File("/a/b/f1.txt")
      val f1Zip = new File("/a/b/f1.zip")
      val f2Txt = new File("/a/b/f2.txt")
      val f2Zip = new File("/a/b/f2.zip")
      val files = List(f1Txt, f1Zip, f2Txt, f2Zip)
      when(webdavFetcher.fetchAll(s"$url/$product/$epoch/$variant", username, password, "product/123/variant")) thenReturn files

      val futureResponse = call(controller.fetch(product, epoch, variant), req)

      val response = await(futureResponse)
      assert(response.header.status === 202)

      testWorker.awaitCompletion()
      verify(webdavFetcher).fetchAll(s"$url/$product/$epoch/$variant", username, password, "product/123/variant")
      verify(unzipper).unzipList(files, "product/123/variant")
      teardown()
    }
  }
}
