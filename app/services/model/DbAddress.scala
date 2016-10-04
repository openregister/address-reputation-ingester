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

import java.util

import scala.annotation.tailrec
import scala.collection.mutable

trait Document {
  def tupled: List[(String, Any)]

  final def toMap = tupled.toMap

  def normalise: Document
}

/**
  * Address typically represents a postal address.
  * For UK addresses, 'town' will always be present.
  * For non-UK addresses, 'town' may be absent and there may be an extra line instead.
  */
// id typically consists of some prefix and the uprn
case class DbAddress(
                      id: String,
                      lines: List[String],
                      town: Option[String],
                      postcode: String,
                      subdivision: Option[String],
                      country: Option[String],
                      localCustodianCode: Option[Int],
                      language: Option[String],
                      blpuState: Option[Int],
                      logicalState: Option[Int],
                      streetClass: Option[Int],
                      latitude: Option[Float],
                      longitude: Option[Float]
                    ) extends Document {

  // UPRN is specified to be an integer of up to 12 digits (it can also be assumed to be always positive)
  def uprn: Long = DbAddress.trimLeadingLetters(id).toLong

  def linesContainIgnoreCase(filterStr: String): Boolean = {
    val filter = filterStr.toUpperCase
    lines.map(_.toUpperCase).exists(_.contains(filter))
  }

  def line1 = if (lines.nonEmpty) lines.head else ""

  def line2 = if (lines.size > 1) lines(1) else ""

  def line3 = if (lines.size > 2) lines(2) else ""

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = {
    List(
      "lines" -> lines,
      "postcode" -> postcode) ++
      town.toList.map("town" -> _) ++
      subdivision.toList.map("subdivision" -> _) ++
      country.toList.map("country" -> _) ++
      localCustodianCode.toList.map("localCustodianCode" -> _) ++
      language.toList.map("language" -> _) ++
      blpuState.toList.map("blpuState" -> _) ++
      logicalState.toList.map("logicalState" -> _) ++
      streetClass.toList.map("streetClass" -> _)
  }

  // We're still providing two structures for the lines, pending a decision on how ES will be used.
  def tupledFlat: List[(String, Any)] = {
    def optLine1 = if (lines.nonEmpty) List(lines.head) else Nil
    def optLine2 = if (lines.size > 1) List(lines(1)) else Nil
    def optLine3 = if (lines.size > 2) List(lines(2)) else Nil
    List(
      "postcode" -> postcode) ++
      optLine1.map("line1" -> _) ++
      optLine2.map("line2" -> _) ++
      optLine3.map("line3" -> _) ++
      town.toList.map("town" -> _) ++
      subdivision.toList.map("subdivision" -> _) ++
      country.toList.map("country" -> _) ++
      localCustodianCode.toList.map("localCustodianCode" -> _) ++
      language.toList.map("language" -> _) ++
      blpuState.toList.map("blpuState" -> _) ++
      logicalState.toList.map("logicalState" -> _) ++
      streetClass.toList.map("streetClass" -> _) ++
      location.toList.map("location" -> _)
  }

  def forMongoDb: List[(String, Any)] = tupled ++ List("_id" -> id)

  def forElasticsearch: Map[String, Any] = tupledFlat.toMap + ("id" -> id)

  def location: Option[String] = {
    for {
      lat <- latitude
      long <- longitude
    } yield s"$lat,$long"
  }

  def splitPostcode = Postcode(postcode)

  def normalise = this
}


object DbAddress {

  import scala.collection.JavaConverters._

  final val English = "en"
  final val Cymraeg = "cy"

  // This is compatible with MongoDBObject.
  def apply(o: mutable.Map[String, AnyRef]): DbAddress = {
    apply(o.toMap)
  }

  // This is compatible with Elasticsearch results.
  def apply(fields: Map[String, AnyRef]): DbAddress = {
    val id = fields.getOrElse("id", fields("_id")).toString
    val linesField = fields.get("lines")

    val lines: List[String] = if (linesField.isDefined) {
      convertLines(linesField.get)
    } else {
      val line1 = fields.getOrElse("line1", "").toString
      val line2 = fields.getOrElse("line2", "").toString
      val line3 = fields.getOrElse("line3", "").toString
      List(line1, line2, line3).filterNot(_.isEmpty)
    }

    DbAddress(
      id,
      lines,
      fields.get("town").map(_.toString),
      fields.getOrElse("postcode", "").toString,
      fields.get("subdivision").map(_.toString),
      fields.get("country").map(_.toString),
      fields.get("localCustodianCode").map(toInteger),
      fields.get("language").map(_.toString),
      fields.get("blpuState").map(toInteger),
      fields.get("logicalState").map(toInteger),
      fields.get("streetClass").map(toInteger),
      fields.get("latitude").map(toFloat),
      fields.get("longitude").map(toFloat)

    )
  }

  private def toInteger(v: Any): Int =
    v match {
      case i: Int => i
      case _ => v.toString.toInt
    }

  private def toFloat(v: Any): Float =
    v match {
      case f: Float => f
      case _ => v.toString.toFloat
    }

  private def convertLines(lines: AnyRef): List[String] = {
    lines match {
      case jl: util.List[_] => (List() ++ jl.asScala).map(_.toString)
      case sl: List[_] => sl.map(_.toString)
    }
  }

  @tailrec
  private[model] def trimLeadingLetters(id: String): String = {
    if (id.isEmpty || Character.isDigit(id.head)) id
    else trimLeadingLetters(id.tail)
  }
}
