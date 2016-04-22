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

import java.nio.file.{Files, Path, Paths}

import org.mockito.Mockito._
import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import org.specs2.mock.Mockito
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.ingester.exec.{WorkQueue, WorkerFactory}
import services.ingester.fetch.WebdavFetcher
import uk.co.hmrc.logging.StubLogger

class FetchControllerTest extends PlaySpec with Mockito with OneAppPerSuite {

  trait context {
    val testWorker = new WorkQueue(new StubLogger())
    val workerFactory = new WorkerFactory {
      override def worker = testWorker
    }
    val webdavFetcher = mock[WebdavFetcher]
    val url = "http://localhost/webdav"
    val username = "foo"
    val password = "bar"
    val outputDirectory: Path = Files.createTempDirectory("fetch-controller-test")
    val controller = new FetchController(workerFactory, webdavFetcher, url, username, password, outputDirectory)
    val req = FakeRequest()

    def teardown() {
      Files.delete(outputDirectory)
    }
  }


  "fetch" should {
    "download files using webdav" in new context {
      val product = "product"
      val epoch = "epoch"
      val variant = "variant"
      val res = call(controller.fetch(product, epoch, variant), req)
      status(res) must be(200)
      testWorker.awaitCompletion()
      verify(webdavFetcher).fetchAll(url, username, password, Paths.get(outputDirectory.toString, product, epoch, variant))
      teardown()
    }
  }
}
