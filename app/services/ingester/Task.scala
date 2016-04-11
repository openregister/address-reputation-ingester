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

import java.util.concurrent.atomic.AtomicBoolean

import play.api.Logger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object Task {
  val singleton = new Task(new LoggerFacade(Logger.logger))
}


class Task(logger: SimpleLogger) {

  private[ingester] val currentlyExecuting: AtomicBoolean = new AtomicBoolean(false)
  private[ingester] val continuing: AtomicBoolean = new AtomicBoolean(false)

  def executeIteration(body: => Unit) {
    if (continuing.get()) {
      body
    }
  }

  def isBusy: Boolean = currentlyExecuting.get

  def awaitCompletion() {
    while (isBusy)
      Thread.sleep(5)
  }

  def abort() {
    continuing.set(false)
  }

  def status: String = {
    // n.b. vals are vital here to avoid race conditions
    val busy = currentlyExecuting.get
    val aborting = !continuing.get
    if (busy && aborting) "busy but aborting"
    else if (busy) "busy"
    else "idle"
  }

  def start(body: => Unit, cleanup: => Unit = {}): Boolean = {
    if (currentlyExecuting.compareAndSet(false, true)) {
      continuing.set(true)
      val f = Future {
        val timer = new DiagnosticTimer
        try {
          scala.concurrent.blocking {
            body
            logger.info(s"Completed after $timer")
          }
        } catch {
          case ie: InterruptedException =>
            logger.info(s"Task has been cancelled after $timer")
        }
      } andThen {
        case r =>
          cleanup
          currentlyExecuting.set(false)
      }
      true
    } else {
      false
    }
  }
}


class TaskFactory {
  def task: Task = Task.singleton
}

