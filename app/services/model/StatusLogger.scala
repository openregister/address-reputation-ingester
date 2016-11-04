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

import java.text.DateFormat
import java.util.Date

import org.slf4j.helpers.MessageFormatter
import uk.gov.hmrc.logging.SimpleLogger

import scala.collection.mutable

class StatusLogger(val tee: SimpleLogger, history: Int = 1) extends SimpleLogger {
  require(history > 0)

  private var buffers = List(new mutable.ListBuffer[String]())

  def info(format: String, arguments: AnyRef*) {
    tee.info(format, arguments: _*)
    pushMessage(format, arguments: _*)
  }

  def warn(format: String, arguments: AnyRef*) {
    tee.warn(format, arguments: _*)
    pushMessage("Warn: " + format, arguments: _*)
  }

  def error(format: String, arguments: AnyRef*) {
    warn(format, arguments: _*)
  }

  private def pushMessage(format: String, arguments: AnyRef*) {
    val message = MessageFormatter.arrayFormat(format, arguments.toArray).getMessage
    synchronized {
      buffers.head += message
    }
  }

  def statusList: List[List[String]] = {
    synchronized {
      buffers.reverse.map(_.toList)
    }
  }

  def status: String = {
    statusList.map(_.mkString("\n")).mkString("\n")
  }

  def startAfresh() {
    val now = DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.MEDIUM).format(new Date())
    synchronized {
      pushMessage(s"~~~~~~~~~~~~~~~ $now ~~~~~~~~~~~~~~~")
      // note that 'buffers' builds up in reverse
      if (buffers.size > history) {
        buffers = buffers.take(history)
      }
      buffers = new mutable.ListBuffer[String]() :: buffers
    }
  }

  def info(msg: String, t: Throwable): Unit = ???

  def warn(msg: String, t: Throwable): Unit = ???

  def error(msg: String, t: Throwable): Unit = ???
}
