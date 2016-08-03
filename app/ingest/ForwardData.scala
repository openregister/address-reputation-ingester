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

import java.io.{Closeable, File}
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
                   preferred: String) extends Closeable {
  def sizeInfo: String =
    s"${blpu.size} BLPUs, ${uprns.size} ${preferred}s, ${streets.size} streets"

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

  private lazy val blpuMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.blpu.mapSize")
  private lazy val blpuValueSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.blpu.valueSize")

  private lazy val dpaSetSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.dpa.setSize")

  private lazy val streetMapSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.street.mapSize")
  private lazy val streetValueSize = mustGetConfigInt(current.mode, current.configuration, "app.chronicleMap.street.valueSize")

  def simpleHeapInstance(preferred: String): ForwardData = new ForwardData(
    new util.HashMap[JLong, String](),
    new util.HashSet[JLong](),
    new util.HashMap[JLong, String](),
    preferred
  )

  def concurrentHeapInstance(preferred: String): ForwardData = new ForwardData(
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    new util.HashSet[JLong](),
    new util.concurrent.ConcurrentHashMap[JLong, String](),
    preferred: String
  )

  def chronicleInMemory(preferred: String): ForwardData = new ForwardData(
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(blpuMapSize).averageValueSize(blpuValueSize).create(),
    ChronicleSetBuilder.of(classOf[JLong]).entries(dpaSetSize).create(),
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(streetMapSize).averageValueSize(streetValueSize).create(),
    preferred: String
  )

  def chronicleInMemoryForUnitTest(preferred: String): ForwardData = new ForwardData(
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(1000).averageValueSize(10).create(),
    ChronicleSetBuilder.of(classOf[JLong]).entries(1000).create(),
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(100).averageValueSize(20).create(),
    preferred: String
  )

  def chronicleWithFile(preferred: String): ForwardData = new ForwardData(
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(blpuMapSize).averageValueSize(blpuValueSize).createPersistedTo(File.createTempFile("blpu", ".dat")),
    ChronicleSetBuilder.of(classOf[JLong]).entries(dpaSetSize).createPersistedTo(File.createTempFile("dpa", ".dat")),
    ChronicleMapBuilder.of(classOf[JLong], classOf[String]).entries(streetMapSize).averageValueSize(streetValueSize).createPersistedTo(File.createTempFile("streets", ".dat")),
    preferred
  )
}
