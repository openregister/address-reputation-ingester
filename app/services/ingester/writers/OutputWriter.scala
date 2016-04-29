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

package services.ingester.writers

import services.ingester.model.ABPModel
import uk.co.hmrc.address.osgb.DbAddress

trait OutputWriter {
  def init(model: ABPModel)

  def output(a: DbAddress)

  def close(): Unit
}

trait OutputWriterFactory {
  def writer(root: String, settings: WriterSettings): OutputWriter
}


case class WriterSettings(bulkSize: Int, loopDelay: Int)

object WriterSettings {
  val default = WriterSettings(1, 0)
}
