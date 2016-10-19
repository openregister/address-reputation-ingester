/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package ingest

import java.io.Closeable
import java.lang.{Long => JLong}
import java.util

import config.ConfigHelper._
import net.openhft.chronicle.map.ChronicleMapBuilder
import net.openhft.chronicle.set.ChronicleSetBuilder
import play.api.Play._

class ForwardData(
                   val blpu: util.Map[JLong, String],
                   val uprns: util.Set[JLong],
                   val streets: util.Map[JLong, String],
                   val streetDescriptorsEn: util.Map[JLong, String],
                   val streetDescriptorsCy: util.Map[JLong, String],
                   val postcodeLCCs: util.Map[String, String],
                   preferred: String) extends Closeable {
  def sizeInfo: String =
    s"${blpu.size} BLPUs, ${uprns.size} ${preferred}s, ${streets.size} streets, ${streetDescriptorsEn.size}/${streetDescriptorsCy.size} street descriptors"

  def numRecords: Int = blpu.size()

  def close() {
    closeCollection(blpu)
    closeCollection(uprns)
    closeCollection(streets)
  }

  private def closeCollection(m: AnyRef) {
    m match {
      case c: Closeable => c.close()
      case _ =>
    }
  }
}


object ForwardData {

  // all sizes are measured in bytes
  private lazy val blpuMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.blpu.mapSize")
  private lazy val dpaSetSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.dpa.setSize")
  private lazy val streetMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.street.mapSize")
  private lazy val streetDescMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.streetDescriptor.mapSize")
  private lazy val postcodeMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.postcode.mapSize")

  private val blpuValueSize = 20
  private val lpiValueSize = 100 // ABP source data has, on average, 141 bytes per record
  private val streetValueSize = 8
  private val streetDescValueSize = 60
  private val postcodeKeySize = 8
  private val postcodeValueSize = 10

  def simpleHeapInstance(preferred: String): ForwardData = new ForwardData(
    new util.HashMap[JLong, String](),
    new util.HashSet[JLong](),
    new util.HashMap[JLong, String](),
    new util.HashMap[JLong, String](),
    new util.HashMap[JLong, String](),
    new util.HashMap[String, String],
    preferred
  )

  def concurrentHeapInstance(preferred: String): ForwardData = new ForwardData(
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    new util.HashSet[JLong](),
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    new util.concurrent.ConcurrentHashMap[String, String](),
    preferred: String
  )

  def chronicleInMemory(preferred: String): ForwardData = new ForwardData(
    mapLongString(blpuMapSize, blpuValueSize),
    ChronicleSetBuilder.of(classOf[JLong]).entries(dpaSetSize).create(),
    mapLongString(streetMapSize, streetValueSize),
    mapLongString(streetDescMapSize, streetDescValueSize),
    mapLongString(streetDescMapSize / 10, streetDescValueSize), // Welsh is much smaller at present
    mapStringString(postcodeMapSize, postcodeKeySize, postcodeValueSize),
    preferred: String
  )

  def chronicleInMemoryForUnitTest(preferred: String): ForwardData = new ForwardData(
    mapLongString(1000, blpuValueSize),
    ChronicleSetBuilder.of(classOf[JLong]).entries(1000).create(),
    mapLongString(100, streetValueSize),
    mapLongString(100, streetDescValueSize),
    mapLongString(10, streetDescValueSize), // Welsh is much smaller at present
    mapStringString(100, postcodeKeySize, postcodeValueSize),
    preferred: String
  )

  private def mapLongString(n: Int, avSize: Int) =
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(n).averageValueSize(avSize).create()

  private def mapStringString(n: Int, keySize: Int, avSize: Int) =
    ChronicleMapBuilder.of(classOf[String], classOf[String]).entries(n).averageKeySize(keySize).averageValueSize(avSize).create()
}
