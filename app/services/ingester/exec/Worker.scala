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

package services.ingester.exec

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{BlockingQueue, LinkedTransferQueue}

import play.api.Logger
import services.ingester.model.{StateModel, StatusLogger}
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.util.control.NonFatal

object ExecutionState {
  // using ints here because of AtomicInteger simplicity
  final val IDLE = 0
  final val BUSY = 1
  final val STOPPING = 2
  final val TERMINATED = 3
}


trait Continuer {
  def isBusy: Boolean
}


case class Task(description: String,
                model: StateModel,
                action: (Continuer) => Unit)


// WorkQueue provids a process-oriented implementation that guarantees the correct interleaving of
// tasks to be performed. These tasks are actioned one at a time. It is possible to abort the
// flow of work cleanly.

object WorkQueue {
    val singleton = new WorkQueue(new LoggerFacade(Logger.logger))
}


class WorkQueue(logger: SimpleLogger) {
  private val queue = new LinkedTransferQueue[Task]()
  private val worker = new Worker(queue, logger)
  worker.setDaemon(true)
  worker.start()

  def push(work: String, model: StateModel, body: (Continuer) => Unit): Boolean = {
    push(Task(work, model, body))
  }

  private def push(task: Task): Boolean = {
    queue.put(task)
    true
  }

  def status: String = worker.status

  def fullStatus: String = worker.fullStatus

  def isBusy: Boolean = worker.isBusy

  def notIdle: Boolean = worker.notIdle

  def hasTerminated: Boolean = worker.hasTerminated

  def abort(): Boolean = worker.abort()

  // used only in special cases, so a simple spin-lock is fine
  def awaitCompletion() {
    Thread.sleep(10)
    while (!queue.isEmpty || notIdle) // these are not quite atomic
      Thread.sleep(5)
  }

  // For application / test shutdown only; there is no way back!
  def terminate() {
    worker.running = false
    abort()
    // push a 'poison' task that may never get execcuted
    push(Task("shutting down", new StateModel(logger), { c => }))
  }
}


private[exec] class Worker(queue: BlockingQueue[Task], logger: SimpleLogger) extends Thread with Continuer {

  import ExecutionState._

  private[exec] var running = true

  private val executionState = new AtomicInteger(IDLE)
  private var doing = "" // n.b. not being used for thread interlocking
  private var statusLogger = new StatusLogger(logger)

  def isBusy: Boolean = executionState.get == BUSY

  def notIdle: Boolean = executionState.get != IDLE

  def hasTerminated: Boolean = executionState.get == TERMINATED

  def abort(): Boolean = {
    executionState.compareAndSet(BUSY, STOPPING)
  }

  def status: String =
    executionState.get match {
      case BUSY => s"busy$doing"
      case STOPPING => s"aborting$doing"
      case _ => "idle"
    }

  def fullStatus: String =
    executionState.get match {
      case BUSY => s"${statusLogger.status}\n\nbusy$doing"
      case STOPPING => s"${statusLogger.status}\n\naborting$doing"
      case _ => s"idle\n\nprevious status:\n${statusLogger.status}"
    }

  override def run() {
    try {
      while (running) {
        doNextTask()
      }
    } finally {
      logger.info("Worker thread has terminated.")
      executionState.set(TERMINATED)
    }
  }

  private def doNextTask() {
    val task = queue.take() // blocks until there is something to do
    executionState.compareAndSet(IDLE, BUSY)
    try {
      runTask(task)
    } catch {
      case NonFatal(e) =>
        logger.warn(status, e)
    } finally {
      if (queue.isEmpty) {
        executionState.set(IDLE)
      }
    }
  }

  private def runTask(task: Task) {
    val info = task.description.trim
    doing = " " + info
    statusLogger = task.model.statusLogger
    statusLogger.info(s"Starting $info")
    try {
      val timer = new DiagnosticTimer
      task.action(this)
      statusLogger.info(s"$info - completed after {}", timer)
    } finally {
      doing = ""
    }
  }
}


class WorkerFactory {
  def worker: WorkQueue = WorkQueue.singleton
}

