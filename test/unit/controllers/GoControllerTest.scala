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

import com.github.sardine.Sardine
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.{Continuer, WorkQueue}
import services.fetch.SardineWrapper
import services.model.{StateModel, StatusLogger}
import services.writers._
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class GoControllerTest extends FunSuite with MockitoSugar {

  val url = new URL("http://localhost/webdav")
  val username = "foo"
  val password = "bar"


  trait context {
    val request = FakeRequest()
    val logger = new StubLogger()
    val status = new StatusLogger(logger)

    val sardine = mock[Sardine]
    val sardineFactory = mock[SardineWrapper]
    when(sardineFactory.begin) thenReturn sardine

    val folder = new File(".")
    val worker = new WorkQueue(new StubLogger())
    val workerFactory = new StubWorkerFactory(worker)

    val fetchController = mock[FetchController]
    val ingestController = mock[IngestController]
    val switchoverController = mock[SwitchoverController]

    val goController = new GoController(logger, workerFactory, sardineFactory, fetchController, ingestController, switchoverController)

    def parameterTest(target: String, product: String, epoch: Int, variant: String): Unit = {
      val writerFactory = mock[OutputFileWriterFactory]
      val request = FakeRequest()

      intercept[IllegalArgumentException] {
        await(call(goController.doGo(target, product, epoch, variant), request))
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

  test("""Given an aborted state,
          doGo should not download files using webdav
          then not unzip every zip file
          and not switch over collections
       """) {
    new context {
      // when
      val response = await(call(goController.doGo("null", "product", 123, "variant"), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[StatusLogger])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(switchoverController, never).switchIfOK(any[StateModel], any[StatusLogger])
      teardown()
    }
  }

  test("""Given a null target,
          doGo should download files using webdav
          then unzip every zip file
          but not switch over collections
       """) {
    new context {
      // when
      val response = await(call(goController.doGo("null", "product", 123, "variant"), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[StatusLogger])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(switchoverController, never).switchIfOK(any[StateModel], any[StatusLogger])
      teardown()
    }
  }

  test("""Given a db target,
          doGo should download files using webdav
          then unzip every zip file
          and then switch over collections
       """) {
    new context {
      // when
      val response = await(call(goController.doGo("db", "product", 123, "variant"), request))

      // then
      worker.awaitCompletion()
      assert(response.header.status === ACCEPTED)
      verify(fetchController).fetch(any[StateModel], any[StatusLogger])
      verify(ingestController).ingestIfOK(any[StateModel], any[StatusLogger], any[WriterSettings], anyString, any[Continuer])
      verify(switchoverController).switchIfOK(any[StateModel], any[StatusLogger])
      teardown()
    }
  }
}
