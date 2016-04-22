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

import play.api.Logger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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


object Worker {
  val singleton = new Worker(new LoggerFacade(Logger.logger))
}


class Worker(logger: SimpleLogger) extends Continuer {

  import ExecutionState._

  private[ingester] val executionState: AtomicInteger = new AtomicInteger(IDLE)
  private var doing = "" // n.b. not being used for thread interlocking

  def isBusy: Boolean = executionState.get == BUSY

  def notIdle: Boolean = executionState.get != IDLE

  def awaitCompletion() {
    while (notIdle)
      Thread.sleep(5)
  }

  def abort(): Boolean = {
    executionState.compareAndSet(BUSY, STOPPING)
  }

  def status: String =
    executionState.get match {
      case BUSY => s"busy$doing"
      case STOPPING => s"aborting$doing"
      case _ => "idle"
    }

  def start(work: String, body: => Unit, cleanup: => Unit = {}): Boolean = {
    start(Task(work, c => body, () => cleanup))
  }

  def start(task: Task): Boolean = {
    if (executionState.compareAndSet(IDLE, BUSY)) {
      doing = " " + task.description.trim
      Future {
        val timer = new DiagnosticTimer
        try {
          scala.concurrent.blocking {
            task.action(this)
            logger.info(s"Task completed after {}", timer)
          }
        } catch {
          case ie: InterruptedException =>
            logger.info(s"Task has been cancelled after {}", timer)
        }
      } andThen {
        case r =>
          task.cleanup()
          executionState.set(IDLE)
          doing = ""
      }
      true
    } else {
      false
    }
  }
}


class WorkerFactory {
  def worker: Worker = Worker.singleton
}

