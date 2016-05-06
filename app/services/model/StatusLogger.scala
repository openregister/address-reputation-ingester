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

package services.model

import org.slf4j.helpers.MessageFormatter
import uk.co.hmrc.logging.SimpleLogger

class StatusLogger(val tee: SimpleLogger) {
  private var buffer = List[String]()
  private var currentStatus = ""

  private def pushMessage(format: String, arguments: AnyRef*) {
    // note that buffer builds up in reverse
    buffer = MessageFormatter.arrayFormat(format, arguments.toArray).getMessage :: buffer
    currentStatus = ""
  }

  def info(format: String, arguments: AnyRef*) {
    tee.info(format, arguments: _*)
    pushMessage(format, arguments: _*)
  }

  def warn(format: String, arguments: AnyRef*) {
    tee.warn(format, arguments: _*)
    pushMessage(format, arguments: _*)
    currentStatus = ""
  }

  def update(s: String) {
    currentStatus = s
  }

  def status: String = {
    val messages = buffer.reverse
    if (currentStatus.isEmpty) messages.mkString("\n")
    else messages.mkString("", "\n", "\n") + currentStatus
  }
}
