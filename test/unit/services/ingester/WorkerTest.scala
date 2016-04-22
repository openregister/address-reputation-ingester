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

package services.ingester

import java.util.concurrent.ArrayBlockingQueue

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import services.ingester.exec.Worker
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class WorkerTest extends FunSuite {

  test(
    """
      when a request to execute something is issued to the task
      and then a second request to execute something is issued to the task
      then the first should return true
      and the second should return false
    """) {
    val logger = new StubLogger()
    val task = new Worker(logger)
    val stuff = new ArrayBlockingQueue[Boolean](1)

    assert(task.status === "idle")

    val started1 = task.start("thinking", {
      stuff.take() // blocks until signalled
      logger.info("foo")
    })

    val started2 = task.start("cogitating", {
      logger.info("bar")
    })

    assert(started1 === true)
    assert(started2 === false)

    assert(task.isBusy)
    assert(task.status === "busy thinking")

    stuff.offer(true) // release the lock
    task.awaitCompletion()

    assert(!task.isBusy)
    assert(task.status === "idle")
  }

  test(
    """
      when a request to execute something is issued to the task
      then two log statements are issued
    """) {
    val logger = new StubLogger()
    val task = new Worker(logger)

    val started = task.start("thinking", {
      logger.info("fric")
    })

    assert(started === true)

    task.awaitCompletion()

    assert(logger.infos.size === 2) // the logger in the body, and the timer in the executor
  }

  test(
    """
      given the execution status is false
      when a request to execute something is issued to the task
      and an interrupted exception occurs in the task
      then return true and log one statement
    """) {
    val logger = new StubLogger()
    val task = new Worker(logger)

    val started = task.start("thinking", {
      throw new InterruptedException("task cancelled")
    })

    assert(started === true)

    task.awaitCompletion()

    assert(logger.infos.size === 1) // the exception handler
  }
}
