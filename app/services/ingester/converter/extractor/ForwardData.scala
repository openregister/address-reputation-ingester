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

import scala.collection.mutable

object ForwardData {
  def empty: ForwardData = ForwardData()
}

case class ForwardData(blpu: mutable.Map[Long, Blpu] = new mutable.HashMap(),
                       dpa: mutable.Set[Long] = new mutable.HashSet(),
                       streets: mutable.Map[Long, Street] = new mutable.HashMap(),
                       lpiLogicStatus: mutable.Map[Long, Byte] = new mutable.HashMap()) {

  def ++(fd: ForwardData): ForwardData = {
    blpu ++= fd.blpu
    dpa ++= fd.dpa
    streets ++= fd.streets
    lpiLogicStatus ++= fd.lpiLogicStatus

    println(s"Forward data: blpu ${blpu.size}, dpa ${dpa.size}, streets ${fd.streets.size}, lpiLogicStatus ${fd.lpiLogicStatus.size}")
    this
  }

  def addBlpus(extraBlpu: mutable.Map[Long, Blpu]): ForwardData = {
    this.blpu ++= extraBlpu
    this
  }

  def addBlpu(extraBlpu: (Long, Blpu)): ForwardData = {
    this.blpu += extraBlpu
    this
  }

  def addDpas(extraDpa: mutable.Set[Long]): ForwardData = {
    this.dpa ++= extraDpa
    this
  }

  def addDpa(extraDpa: Long): ForwardData = {
    this.dpa += extraDpa
    this
  }

  def addStreets(extraStreets: mutable.Map[Long, Street]): ForwardData = {
    this.streets ++= extraStreets
    this
  }

  def addStreet(extraStreet: (Long, Street)): ForwardData = {
    this.streets += extraStreet
    this
  }

  def addLpiLogicStatuses(extraLpi: mutable.Map[Long, Byte]): ForwardData = {
    this.lpiLogicStatus ++= extraLpi
    this
  }

  def addLpiLogicStatus(extraLpi: (Long, Byte)): ForwardData = {
    this.lpiLogicStatus += extraLpi
    this
  }
}

