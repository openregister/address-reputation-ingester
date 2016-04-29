/*
 * Copyright 2016 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package services.ingester.model

import org.slf4j.helpers.MessageFormatter
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.mutable

class StatusLogger(val tee: SimpleLogger) {
  private val buffer = new mutable.ListBuffer[String]()

  private var currentStatus = ""
  private var giveUp = false

  def put(format: String, arguments: AnyRef*) {
    tee.info(format, arguments: _*)
    buffer += MessageFormatter.arrayFormat(format, arguments.toArray).getMessage
    currentStatus = ""
  }

  def fail(format: String, arguments: AnyRef*) {
    tee.warn(format, arguments: _*)
    buffer += MessageFormatter.arrayFormat(format, arguments.toArray).getMessage
    currentStatus = ""
    giveUp = true
  }

  def hasFailed: Boolean = giveUp

  def update(s: String) {
    currentStatus = s
  }

  def status: String =
    if (currentStatus.isEmpty) buffer.mkString("\n")
    else buffer.mkString("", "\n", "\n") + currentStatus
}
