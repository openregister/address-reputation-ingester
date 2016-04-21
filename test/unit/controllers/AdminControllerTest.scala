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

import java.util.concurrent.ArrayBlockingQueue

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import play.api.mvc.{AnyContent, Request}
import play.api.test.FakeRequest
import services.ingester.exec.Task
import uk.co.hmrc.logging.StubLogger
import play.api.test.FakeRequest
import play.api.test.Helpers._

@RunWith(classOf[JUnitRunner])
class AdminControllerTest extends org.scalatest.FunSuite {

  test(
    """
      when cancel task is called
      and no task is executing
      then a bad request response is returned
    """) {
    val logger = new StubLogger
    val ac = new AdminController(new Task(logger))
    val request = FakeRequest()

    val futureResult = call(ac.cancelTask(), request)

    val result = await(futureResult)
    assert(result.header.status === 400)
  }

  test(
    """
      when cancel task is called
      and a task is executing
      then a successful response is returned
    """) {
    val logger = new StubLogger
    val stuff = new ArrayBlockingQueue[Boolean](1)
    val task = new Task(logger)
    task.start("thinking", {
      stuff.take() // blocks until signalled
    })

    val ac = new AdminController(task)
    val request = FakeRequest()

    val futureResult = call(ac.cancelTask(), request)

    val result = await(futureResult)
    assert(result.header.status === 200)
    stuff.offer(true) // release the lock
  }
}
