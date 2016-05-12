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

package services.writers

import java.util.Date

import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress

class OutputNullWriter(model: StateModel, statusLogger: StatusLogger) extends OutputWriter {

  private var count = 0

  def existingTargetThatIsNewerThan(date: Date): Option[String] = None

  def begin() {}

  def output(a: DbAddress) {
    count += 1
  }

  // scalastylye:off
  def end(completed: Boolean): StateModel = {
    statusLogger.info(s"*** document count = $count")
    model
  }
}


class OutputNullWriterFactory extends OutputWriterFactory {
  override def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputWriter =
    new OutputNullWriter(model, statusLogger)
}
