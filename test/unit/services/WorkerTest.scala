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

package services

import java.util.concurrent.SynchronousQueue

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import services.exec.WorkQueue
import services.model.StatusLogger
import uk.gov.hmrc.logging.StubLogger

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
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock1 = new SynchronousQueue[Boolean]()
    val lock2 = new SynchronousQueue[Boolean]()

    assert(worker.status === "idle")

    worker.push("thinking", {
      continuer =>
        lock1.take() // blocks until signalled
        logger.info("foo")
        lock1.take()
    })

    worker.push("cogitating", {
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
      when the worker is busy
      and several additional requests are enqueued
      then viewQueue method returns the expected list of enqueued tasks.
    """) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock1 = new SynchronousQueue[Boolean]()

    assert(worker.status === "idle")

    worker.push("thinking", {
      continuer =>
        lock1.take() // blocks until signalled
        lock1.take()
    })

    worker.push("cogitating1", {
      continuer =>
        logger.info("bar")
    })

    worker.push("cogitating2", {
      continuer =>
        logger.info("bar")
    })

    worker.push("cogitating3", {
      continuer =>
        logger.info("bar")
    })

    lock1.put(true)

    assert(worker.viewQueue.map(_.description) === List("busy thinking", "cogitating1", "cogitating2", "cogitating3"))

    // clean up
    lock1.put(true)
    worker.awaitCompletion()

    assert(worker.viewQueue === List(WorkQueue.idle))

    assert(!worker.isBusy)
    assert(worker.status === "idle")
  }

  test(
    """
      when the worker is busy
      and several additional requests are enqueued
      then dropQueueItem method removes an item from the list
      and returns the list of remaining enqueued tasks.
    """) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock1 = new SynchronousQueue[Boolean]()

    assert(worker.status === "idle")

    worker.push("thinking", {
      continuer =>
        lock1.take() // blocks until signalled
        lock1.take()
    })

    worker.push("cogitating1", {
      continuer =>
        logger.info("bar")
    })

    worker.push("cogitating2", {
      continuer =>
        logger.info("bar")
    })

    worker.push("cogitating3", {
      continuer =>
        logger.info("bar")
    })

    val toBeDeleted = worker.viewQueue.find(_.description == "cogitating2").get

    lock1.put(true)

    val remaining = worker.dropQueueItem(toBeDeleted.id.get)
    assert(remaining.map(_.description) === List("busy thinking", "cogitating1", "cogitating3"))

    // clean up
    lock1.put(true)
    worker.awaitCompletion()

    assert(worker.viewQueue === List(WorkQueue.idle))

    assert(!worker.isBusy)
    assert(worker.status === "idle")
  }

  test(
    """
      when a request to execute something is issued to the worker
      then two log statements are issued
    """) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock = new SynchronousQueue[Boolean]()

    worker.push("thinking", {
      continuer =>
        logger.info("fric")
        lock.put(true)
    })

    lock.take()
    worker.awaitCompletion()

    // the logger in the body, and the timer in the executor
    assert(logger.infos.size === 3, logger.all.mkString(",\n"))
  }

  test(
    """
      given the execution status is idle
      when a request to execute something is issued to the worker
      and an exception occurs in the worker
      then return true and log one statement
    """) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock = new SynchronousQueue[Boolean]()

    worker.push("thinking", {
      continuer =>
        lock.put(true)
        throw new Exception("worker broke")
    })

    lock.take()
    worker.awaitCompletion()

    // the exception handler
    assert(logger.warns.size === 2, logger.all.mkString(",\n"))
  }

  test(
    """
      given the execution status is idle
      when the worker thread is terminated
      then its thread terminates
    """) {
    val logger = new StubLogger()
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)

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
    val status = new StatusLogger(logger, 1)
    val worker = new WorkQueue(status)
    val lock = new SynchronousQueue[Boolean]()

    worker.push("thinking", {
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
