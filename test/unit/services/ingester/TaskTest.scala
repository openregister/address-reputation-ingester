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

import org.scalatest.FunSuite
import uk.co.hmrc.logging.{SimpleLogger, StubLogger}

import scala.concurrent.ExecutionContext.Implicits.global

class TaskTest extends FunSuite {

  test(
    """
      given the execution status is true
      when a request to execute something is issued to the task
      then return false
    """) {
    Task.currentlyExecuting.set(true)
    val logger = new StubLogger()
    val task = new Task(logger)

    val status = task.execute((logger: SimpleLogger) => {
      logger.info("fric")
    })

    assert(status === false)
  }

  test(
    """
      given the execution status is false
      when a request to execute something is issued to the task
      then return true and log two statements
    """) {
    Task.currentlyExecuting.set(false)
    val logger = new StubLogger()
    val task = new Task(logger)

    val status = task.execute((logger: SimpleLogger) => {
      logger.info("fric")
    })

    assert(status === true)

    task.f map { f =>
      assert(logger.infos.size === 2) // the logger in the body, and the timer in the executor
    }

  }

  test(
    """
      given the execution status is false
      when a request to execute something is issued to the task
      and an interrupted exception occurs in the task
      then return true and log one statement
    """) {
    Task.currentlyExecuting.set(false)
    val logger = new StubLogger()
    val task = new Task(logger)

    val status = task.execute((logger: SimpleLogger) => {
      throw new InterruptedException("task cancelled")
    })

    assert(status === true)

    task.f map { f =>
      assert(logger.infos.size === 1) // the exception handler
    }
  }
}
