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

import java.util.concurrent.SynchronousQueue

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import services.ingester.exec.WorkQueue
import services.ingester.model.StateModel
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class WorkerTest extends FunSuite {

  test(
    """
      when a request to execute something is issued to the worker
      and then a second request to execute something is issued to the worker
      then the first should execute to completion
      and then the second should execute to completion
    """) {
    val logger = new StubLogger()
    val worker = new WorkQueue(logger)
    val lock1 = new SynchronousQueue[Boolean]()
    val lock2 = new SynchronousQueue[Boolean]()
    val model = new StateModel(logger)

    assert(worker.status === "idle")

    worker.push("thinking", model, {
      continuer =>
        lock1.take() // blocks until signalled
        logger.info("foo")
        lock1.take()
    })

    worker.push("cogitating", model, {
      continuer =>
        lock2.take()
        logger.info("bar")
        lock2.take()
    })

    lock1.put(true)
    assert(worker.isBusy)
    assert(worker.status === "busy thinking")
    lock1.put(true) // release the first lock

    lock2.put(true) // release the second lock
    assert(worker.isBusy)
    assert(worker.status === "busy cogitating")
    lock2.put(true) // release the second lock

    worker.awaitCompletion()

    assert(!worker.isBusy)
    assert(worker.status === "idle")
  }

  test(
    """
      when a request to execute something is issued to the worker
      then two log statements are issued
    """) {
    val logger = new StubLogger()
    val worker = new WorkQueue(logger)
    val lock = new SynchronousQueue[Boolean]()
    val model = new StateModel(logger)

    worker.push("thinking", model, {
      continuer =>
        logger.info("fric")
        lock.put(true)
    })

    lock.take()
    worker.awaitCompletion()

    // the logger in the body, and the timer in the executor
    assert(logger.infos.size === 2, logger.all.mkString(",\n"))
  }

  test(
    """
      given the execution status is idle
      when a request to execute something is issued to the worker
      and an exception occurs in the worker
      then return true and log one statement
    """) {
    val logger = new StubLogger()
    val worker = new WorkQueue(logger)
    val lock = new SynchronousQueue[Boolean]()
    val model = new StateModel(logger)

    worker.push("thinking", model, {
      continuer =>
        lock.put(true)
        throw new Exception("worker broke")
    })

    lock.take()
    worker.awaitCompletion()

    // the exception handler
    assert(logger.warns.size === 1, logger.all.mkString(",\n"))
  }

  test(
    """
      given the execution status is idle
      when the worker thread is terminated
      then its thread terminates
    """) {
    val logger = new StubLogger()
    val worker = new WorkQueue(logger)

    worker.terminate()

    var patience = 100
    while (!worker.hasTerminated && patience > 0) {
      Thread.sleep(10)
      patience -= 1
    }
    Thread.sleep(100)
    assert(patience != 0, "Ran out of patience waiting for termination")
    assert(logger.infos.nonEmpty, logger.all.mkString(",\n"))
  }

  test(
    """
      given the execution status is busy
      when the worker thread is terminated
      then its thread terminates
    """) {
    val logger = new StubLogger()
    val worker = new WorkQueue(logger)
    val lock = new SynchronousQueue[Boolean]()
    val model = new StateModel(logger)

    worker.push("thinking", model, {
      continuer =>
        lock.put(true)
        lock.put(true)
    })

    lock.take()
    worker.terminate()
    lock.take()

    var patience = 100
    while (!worker.hasTerminated && patience > 0) {
      Thread.sleep(10)
      patience -= 1
    }
    Thread.sleep(10)
    assert(patience != 0, "Ran out of patience waiting for termination")
    assert(logger.infos.nonEmpty, logger.all.mkString(",\n"))
  }
}
