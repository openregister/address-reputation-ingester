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

package services.exec

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{BlockingQueue, LinkedTransferQueue}

import play.api.Logger
import services.model.StatusLogger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.LoggerFacade

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


case class TaskInfo(id: Option[Int], description: String)


case class Task(info: TaskInfo, action: (Continuer) => Unit)


object Task {
  private val idGenerator = new AtomicInteger()

  def apply(description: String, action: (Continuer) => Unit): Task = {
    new Task(TaskInfo(Some(idGenerator.incrementAndGet), description), action)
  }
}


// WorkQueue provids a process-oriented implementation that guarantees the correct interleaving of
// tasks to be performed. These tasks are actioned one at a time. It is possible to abort the
// flow of work cleanly.

object WorkQueue {
  val singleton = new WorkQueue(new StatusLogger(new LoggerFacade(Logger.logger)))

  val idle = TaskInfo(None, "idle")
}


class WorkQueue(val statusLogger: StatusLogger) {
  private val queue = new LinkedTransferQueue[Task]()
  private val worker = new Worker(queue, statusLogger)
  worker.setDaemon(true)
  worker.start()

  def push(work: String, body: (Continuer) => Unit): Boolean = {
    push(Task(work, body))
  }

  private def push(task: Task): Boolean = {
    queue.put(task)
    true
  }

  import scala.collection.mutable

  def viewQueue: List[TaskInfo] = {
    val b = new mutable.ListBuffer[TaskInfo]
    b.append(worker.statusInfo)
    val it = queue.iterator
    while (it.hasNext) {
      val item = it.next
      b.append(item.info)
    }
    b.toList
  }

  def dropQueueItem(id: Int): List[TaskInfo] = {
    val b = new mutable.ListBuffer[TaskInfo]
    b.append(worker.statusInfo)
    val it = queue.iterator
    while (it.hasNext) {
      val item = it.next
      if (item.info.id.contains(id)) {
        it.remove()
      } else {
        b.append(item.info)
      }
    }
    b.toList
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
    push(Task("shutting down", { c: Continuer => }))
  }
}


private[exec] class Worker(queue: BlockingQueue[Task], statusLogger: StatusLogger) extends Thread with Continuer {

  import ExecutionState._

  private[exec] var running = true

  private val _currentInfo = new AtomicReference(WorkQueue.idle)
  private val _executionState = new AtomicInteger(IDLE)

  def isBusy: Boolean = _executionState.get == BUSY

  def notIdle: Boolean = _executionState.get != IDLE

  def hasTerminated: Boolean = _executionState.get == TERMINATED

  def abort(): Boolean = {
    val b = _executionState.compareAndSet(BUSY, STOPPING)
    statusLogger.warn(s"Abort. Now $status.")
    b
  }

  private def stateName =
    _executionState.get match {
      case BUSY => "busy"
      case STOPPING => "aborting"
      case _ => ""
    }

  // n.b. there appears to be a startup racce condition in which the _currentInfo.get might be null
  // (otherwise it is never null).
  def currentInfo: TaskInfo = Option(_currentInfo.get).getOrElse(WorkQueue.idle)

  private def doing = currentInfo.description

  def status: String = (stateName + " " + doing).trim

  def statusInfo: TaskInfo = TaskInfo(None, status)

  def fullStatus: String = s"${statusLogger.status}\n\n$status"

  override def run() {
    try {
      while (running) {
        doNextTask()
      }
    } finally {
      statusLogger.info("Worker thread has terminated.")
      _executionState.set(TERMINATED)
    }
  }

  private def doNextTask() {
    val task = queue.take() // blocks until there is something to do
    _executionState.compareAndSet(IDLE, BUSY)
    assert(task.info!=null)
    _currentInfo.set(task.info)
    statusLogger.startAfresh()
    try {
      runTask(task)
    } catch {
      case NonFatal(e) =>
        statusLogger.warn(status, e)
    } finally {
      _currentInfo.set(WorkQueue.idle)
      if (queue.isEmpty) {
        _executionState.set(IDLE)
      }
    }
  }

  private def runTask(task: Task) {
    val info = doing
    statusLogger.info(s"Starting $info.")
    try {
      val timer = new DiagnosticTimer
      task.action(this)
      statusLogger.info(s"Finished $info after {}.", timer)
    } catch {
      case re: RuntimeException =>
        statusLogger.warn(re.getClass.getName + " " + re.getMessage)
        statusLogger.tee.warn(info, re)
        throw re
      case e: Exception =>
        statusLogger.warn(e.getClass.getName + " " + e.getMessage)
        throw e
    }
  }
}


class WorkerFactory {
  def worker: WorkQueue = WorkQueue.singleton
}

