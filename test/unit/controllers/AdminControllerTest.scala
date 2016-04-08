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

package controllers

import services.ingester.Task

class AdminControllerTest extends org.scalatest.FunSuite {

  test(
    """
      when cancel task is called
      and no task is executing
      then a bad request response is returned
    """) {
    Task.currentlyExecuting.set(false)
    val ac = new AdminController()
    val result = ac.handleCancelTask(null)

    //cleanup side effects globals
    Task.cancelTask.set(false)
    Task.currentlyExecuting.set(false)
    assert(result.header.status === 400)
  }

  test(
    """
      when cancel task is called
      and a task is executing
      then a successful response is returned
    """) {
    Task.currentlyExecuting.set(true)
    val ac = new AdminController()
    val result = ac.handleCancelTask(null)

    //cleanup side effects globals
    Task.cancelTask.set(false)
    Task.currentlyExecuting.set(false)
    assert(result.header.status === 200)
  }
}
