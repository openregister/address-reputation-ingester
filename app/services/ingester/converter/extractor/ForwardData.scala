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

import java.io.File
import java.lang.{Long => JLong}
import java.util

import net.openhft.chronicle.map.ChronicleMapBuilder
import net.openhft.chronicle.set.ChronicleSetBuilder

class ForwardData(
                   val blpu: util.Map[JLong, String],
                   val dpa: util.Set[JLong],
                   val streets: util.Map[JLong, String]) {
  def sizeInfo: String =
    s"${blpu.size} BLPUs, ${dpa.size} DPAs, ${streets.size} streets"
}


object ForwardData {

  def simpleInstance(): ForwardData = new ForwardData(
    new util.HashMap[JLong, String](),
    new util.HashSet[JLong](),
    new util.HashMap[JLong, String]()
  )

  def chronicleInMemory(): ForwardData = new ForwardData(
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(35000000).averageValueSize(10).create(),
    ChronicleSetBuilder.of(classOf[JLong]).entries(35000000).create(),
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(1500000).averageValueSize(20).create()
  )

  def chronicleWithFile(): ForwardData = new ForwardData(
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(35000000).averageValueSize(10).createPersistedTo(File.createTempFile("blpu", ".dat")),
    ChronicleSetBuilder.of(classOf[JLong]).entries(35000000).createPersistedTo(File.createTempFile("dpa", ".dat")),
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(1500000).averageValueSize(20).createPersistedTo(File.createTempFile("streets", ".dat"))
  )
}
