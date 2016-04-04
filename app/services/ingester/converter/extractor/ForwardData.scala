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

package services.ingester.converter.extractor

import services.ingester.converter.Extractor.{Blpu, Street}

import scala.collection.immutable.{HashMap, HashSet}

object ForwardData {
  def empty: ForwardData = ForwardData(HashMap.empty[Long, Blpu], HashSet.empty[Long], HashMap.empty[Long, Street], HashMap.empty[Long, Byte])
}

case class ForwardData(blpu: HashMap[Long, Blpu],
                       dpa: HashSet[Long],
                       streets: HashMap[Long, Street],
                       lpiLogicStatus: HashMap[Long, Byte]) {

  def update(fd: ForwardData): ForwardData = {
    val totalDpa = dpa ++ fd.dpa
    val totalBlpu = blpu ++: fd.blpu
    val remainingBlpu = totalDpa.foldLeft(totalBlpu) { (b, d) => b - d }
    val remainingDpa = totalDpa -- fd.blpu.keySet // just try and keep the memory down, fill not delete everything

    ForwardData(remainingBlpu, remainingDpa, fd.streets, fd.lpiLogicStatus)
  }
}

