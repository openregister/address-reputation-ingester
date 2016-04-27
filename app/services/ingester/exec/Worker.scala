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
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

object ExecutionState {
  // using ints here because of AtomicInteger simplicity
  final val IDLE = 0
  final val BUSY = 1
  final val STOPPING = 2
}


trait Continuer {
  def isBusy: Boolean
}


case class Task(description: String,
                action: (Continuer) => Unit,
                cleanup: () => Unit = () => {})


object WorkQueue {
  val singleton: WorkQueue = throw new RuntimeException()
//  val singleton: WorkQueue = new WorkQueue(new LoggerFacade(Logger.logger))
}


class WorkQueue(logger: SimpleLogger) {
  private val queue = new LinkedTransferQueue[Task]()
  private val worker = new Worker(queue, logger)
  worker.setDaemon(true)
  worker.start()

  def push(work: String, body: => Unit, cleanup: => Unit = {}): Boolean = {
    push(Task(work, c => body, () => cleanup))
  }

  def push(task: Task): Boolean = {
    queue.offer(task)
  }

  def status: String = worker.status

  def isBusy: Boolean = worker.isBusy

  def notIdle: Boolean = worker.notIdle

  def abort(): Boolean = worker.abort()

  // used only in special cases, so a simple spin-lock is fine
  def awaitCompletion() {
    while (notIdle)
      Thread.sleep(5)
  }

  // for application / test shutdown only
  def terminate() {
    worker.running = false
  }
}


private[exec] class Worker(queue: BlockingQueue[Task], logger: SimpleLogger) extends Thread with Continuer {

  import ExecutionState._

  private[exec] var running = true

  private val executionState = new AtomicInteger(IDLE)
  private var doing = "" // n.b. not being used for thread interlocking

  def isBusy: Boolean = executionState.get == BUSY

  def notIdle: Boolean = executionState.get != IDLE

  def abort(): Boolean = {
    executionState.compareAndSet(BUSY, STOPPING)
  }

  def status: String =
    executionState.get match {
      case BUSY => s"busy$doing"
      case STOPPING => s"aborting$doing"
      case _ => "idle"
    }

  override def run() {
    while (running) {
      doNextTask()
    }
  }

  private def doNextTask() {
    val task = queue.take() // blocks until there is something to do
    executionState.compareAndSet(IDLE, BUSY)
    try {
      runTask(task)
    } catch {
      case i: InterruptedException =>
        throw i // for terminating the containing thread
      case e: Exception =>
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
    try {
      val timer = new DiagnosticTimer
      task.action(this)
      logger.info(s"$info - completed after {}", timer)
    } finally {
      task.cleanup()
      doing = ""
    }
  }
}


class WorkerFactory {
  def worker: WorkQueue = WorkQueue.singleton
}

