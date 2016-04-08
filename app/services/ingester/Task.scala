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
  //TODO: change to enum to reflect different execution states
  var currentlyExecuting: AtomicBoolean = new AtomicBoolean(false)
  var cancelTask: AtomicBoolean = new AtomicBoolean(false)

  def executeIteration(body: => Unit): Unit = {
    if (!cancelTask.get()) {
      body
    } else {
      cancelTask.set(false)
      throw new InterruptedException("Execution cancelled")
    }
  }
}


class Task(logger: SimpleLogger = new LoggerFacade(Logger.logger)) {

  var f: Future[Unit] = null

  def execute(body: (SimpleLogger) => Unit, cleanup: => Unit = {}): Boolean = {
    if (Task.currentlyExecuting.compareAndSet(false, true)) {
      f = Future {
        try {
          scala.concurrent.blocking {
            val timer = new DiagnosticTimer
            body(logger)
            logger.info(timer.toString)
          }
        } catch {
          case ie: InterruptedException => {
            logger.info("Task has been cancelled")
          }
        }
      } andThen {
        case r => {
          cleanup
          Task.currentlyExecuting.set(false)
        }
      }
      true
    } else {
      false
    }
  }
}

class TaskFactory {
  def task: Task = new Task
}

