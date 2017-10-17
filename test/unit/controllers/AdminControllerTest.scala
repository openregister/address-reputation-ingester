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
 */

package controllers

import java.util.concurrent.SynchronousQueue

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.scalatest.mock.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.inject.ApplicationLifecycle
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.WorkQueue
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.logging.StubLogger

class AdminControllerTest extends PlaySpec with MockitoSugar {

  implicit val system = ActorSystem("test")

  implicit def mat: Materializer = ActorMaterializer()

  class Scenario {
    val logger = new StubLogger
    val statusLogger = new StatusLogger(logger)
    val lifecycle = mock[ApplicationLifecycle]
    val logFileHelper = new LogFileHelper
    val cc = mock[ControllerConfig]
    val dirTreeHelper = new DirTreeHelper(cc)
    val worker = new WorkQueue(lifecycle, statusLogger)
    val ac = new AdminController(worker, dirTreeHelper, logFileHelper, cc)
    val request = FakeRequest()
  }

  "cancel task" should {

    "return ok when no task is executing" in new Scenario {
      val futureResponse = call(ac.cancelTask(), request)
      status(futureResponse) must be (200)
      worker.terminate()
    }

    "return accepted when a task is executing" in new Scenario {
      val stuff = new SynchronousQueue[Boolean]()
      val model = new StateModel()
      worker.push("thinking", {
        c =>
          stuff.take() // blocks until signalled
          stuff.take() // blocks until signalled
      })
      stuff.put(true)
      val futureResponse = call(ac.cancelTask(), request)
      status(futureResponse) must be (202)
      stuff.put(true) // release the lock second time
      worker.awaitCompletion()
      worker.terminate()
    }

  }

}
