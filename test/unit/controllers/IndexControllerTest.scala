/*
 * Copyright 2017 HM Revenue & Customs
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

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.inject.ApplicationLifecycle
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.WorkQueue
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es.IndexMetadata
import uk.gov.hmrc.logging.StubLogger


@RunWith(classOf[JUnitRunner])
class IndexControllerTest extends FunSuite with MockitoSugar {
  implicit val system = ActorSystem("test")

  implicit def mat: Materializer = ActorMaterializer()

  trait Context {
    protected val idxName = "010101"
    private val logger = new StubLogger
    private val status = new StatusLogger(logger)
    private val lifecycle = mock[ApplicationLifecycle]
    protected val worker = new WorkQueue(lifecycle, status)
    protected val request = FakeRequest()

    protected val indexMetadata: IndexMetadata = mock[IndexMetadata]
    when(indexMetadata.indexExists(any())) thenReturn true
    when(indexMetadata.toggleDoNotDelete(any())) thenCallRealMethod()

    protected val ic = new IndexController(status, worker, indexMetadata)
  }

  test(
    """
      when doNotDelete is called
      then an accepted response is returned
    """) {
    new Context {
      private val futureResponse = call(ic.doDoNotDelete(idxName), request)

      private val response = await(futureResponse)
      assert(response.header.status === 202)
      worker.terminate()
    }
  }

  test(
    """
      when doNotDelete is called
      for an index which doesn't exist
      then a not found response is returned
    """) {

    new Context {
      when(indexMetadata.indexExists(any())) thenReturn false

      private val futureResponse = call(ic.doDoNotDelete(idxName), request)

      private val response = await(futureResponse)
      assert(response.header.status === 404)
      worker.terminate()
    }
  }

  test(
    """
      when doCleanup is called
      then an accepted response is returned
    """) {
    new Context {
      private val futureResponse = call(ic.doCleanup(), request)

      private val response = await(futureResponse)
      assert(response.header.status === 202)
      worker.terminate()
    }
  }
}
