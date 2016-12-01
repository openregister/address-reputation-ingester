/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package controllers

import java.io.File
import java.net.URL

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import com.github.sardine.Sardine
import fetch.{FetchController, SardineWrapper, WebDavFile, WebDavTree}
import ingest.writers._
import ingest.{IngestController, StubWorkerFactory}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.writers.WriterSettings
import uk.gov.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class GoControllerTest extends FunSuite with MockitoSugar {

  import WebDavTree.readyToCollectFile

  val base = "http://somedavserver.com:81/webdav"
  val baseUrl = new URL(base + "/")
  val username = "foo"
  val password = "bar"

  implicit val system = ActorSystem("test")
  implicit def mat: Materializer = ActorMaterializer()

  trait context {
    val request = FakeRequest()
    val logger = new StubLogger()
    val status = new StatusLogger(logger)

    val sardine = mock[Sardine]
    val sardineWrapper = mock[SardineWrapper]
    when(sardineWrapper.begin) thenReturn sardine

    val folder = new File(".")
    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)

    val fetchController = mock[FetchController]
    val ingestController = mock[IngestController]
    val esSwitchoverController = mock[SwitchoverController]
    val esIndexController = mock[IndexController]

    val goController = new GoController(status, workerFactory, sardineWrapper,
      fetchController, ingestController,
      esSwitchoverController, esIndexController)

    def parameterTest(target: String, product: String, epoch: Int, variant: String): Unit = {
      val writerFactory = mock[OutputFileWriterFactory]
      val request = FakeRequest()

      intercept[IllegalArgumentException] {
        await(call(goController.doGo(target, product, epoch, variant, None, None, None), request))
      }
    }

    def teardown() {
      worker.terminate()
    }
  }


  test(
    """
       when an invalid target is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("bong", "abi", 40, "full")
    }
  }

  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("null", "$%", 40, "full")
    }
  }

  test(
    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("null", "abi", 40, ")(")
    }
  }

  test(
    """Given an aborted state,
          doGo should not download files using webdav
          then not unzip every zip file
          and not switch over indexes
    """) {
    new context {
      // when
      val response = await(call(goController.doGo("null", "product", 123, "variant", None, None, None), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[Continuer])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(esSwitchoverController, never).switchIfOK(any[StateModel])
      teardown()
    }
  }

  test(
    """Given a null target,
          doGo should download files using webdav
          then unzip every zip file
          but not switch over indexes
    """) {
    new context {
      // when
      val response = await(call(goController.doGo("null", "product", 123, "variant", None, None, None), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[Continuer])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(esSwitchoverController, never).switchIfOK(any[StateModel])
      teardown()
    }
  }

  test(
    """Given an es target,
          doGo should download files using webdav
          then unzip every zip file
          and then switch over indexes
    """) {
    new context {
      // when
      val response = await(call(goController.doGo("es", "product", 123, "variant", None, None, None), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[Continuer])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(esSwitchoverController).switchIfOK(any[StateModel])
      teardown()
    }
  }

  test(
    """Given an es target and a tree containing abpi and abp files
          doGoAuto should find remote files
          then download files for both products using webdav
          then unzip every zip file
          and then switch over indexes
    """) {
    new context {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abi/38/"), "38", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abi/38/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abi/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true),
                WebDavFile(new URL(base + "/abi/38/full/" + readyToCollectFile), readyToCollectFile, isPlainText = true)
              ))
            )))),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true, files = List(
            WebDavFile(new URL(base + "/abp/38/"), "38", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/38/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true),
                WebDavFile(new URL(base + "/abp/38/full/" + readyToCollectFile), readyToCollectFile, isPlainText = true)
              ))
            )),
            WebDavFile(new URL(base + "/abp/39/"), "39", isDirectory = true, files = List(
              WebDavFile(new URL(base + "/abp/39/full/"), "full", isDirectory = true, files = List(
                WebDavFile(new URL(base + "/abp/39/full/DVD1.zip"), "DVD1.zip", isDataFile = true),
                WebDavFile(new URL(base + "/abp/39/full/DVD2.zip"), "DVD2.zip", isDataFile = true),
                WebDavFile(new URL(base + "/abp/39/full/" + readyToCollectFile), readyToCollectFile, isPlainText = true)
              ))
            ))
          ))
        )))
      when(sardineWrapper.exploreRemoteTree) thenReturn tree

      // when
      val response = await(call(goController.doGoAuto("es", None, None), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController, times(2)).fetch(any[StateModel], any[Continuer])
      verify(ingestController, times(2)).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(esSwitchoverController, times(2)).switchIfOK(any[StateModel])
      verify(esIndexController).cleanup()
      teardown()
    }
  }

  test(
    """Given a db target and a tree containing no files
          doGoAuto should find no remote files
    """) {
    new context {
      // given
      val tree = WebDavTree(
        WebDavFile(new URL(base + "/"), "webdav", isDirectory = true, files = List(
          WebDavFile(new URL(base + "/abi/"), "abi", isDirectory = true),
          WebDavFile(new URL(base + "/abp/"), "abp", isDirectory = true)
        )))
      when(sardineWrapper.exploreRemoteTree) thenReturn tree

      // when
      val response = await(call(goController.doGoAuto("es", None, None), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController, never).fetch(any[StateModel], any[Continuer])
      verify(ingestController, never).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(esSwitchoverController, never).switchIfOK(any[StateModel])
      teardown()
    }
  }
}
