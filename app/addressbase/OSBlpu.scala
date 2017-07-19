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
 *
 */

package addressbase

import ingest.Ingester
import uk.gov.hmrc.address.osgb.Document
import uk.gov.hmrc.address.uk.Postcode

object OSBlpu {
  val RecordId = "21"

  // scalastyle:off
  var idx = OSBlpuIdx(
    uprn = 3,
    parentUprn = 7,
    logicalState = 4,
    blpuState = 5,
    subdivision = 14,
    localCustodian = 13,
    postalCode = 19,
    postcode = 20,
    latitude = 10,
    longitude = 11)

  def isUsefulPostcode(csv: Array[String]): Boolean = {
    csv(idx.postalCode) != "N" // not a postal address
  }

  def apply(csv: Array[String]): OSBlpu = {
    val subdivision = csv(idx.subdivision).head
    val lat = csv(idx.latitude)
    val long = csv(idx.longitude)
    OSBlpu(
      csv(idx.uprn).toLong,
      blankToOptLong(csv(idx.parentUprn)),
      toChar(csv(idx.logicalState)),
      toChar(csv(idx.blpuState)),
      subdivision,
      csv(idx.postcode),
      csv(idx.localCustodian).toInt,
      lat,
      long
    )
  }

  private def toChar(s: String) = if (s.isEmpty) ' ' else s.head
}

case class OSBlpuIdx(uprn: Int, parentUprn: Int, logicalState: Int, blpuState: Int, subdivision: Int,
                     localCustodian: Int, postalCode: Int, postcode: Int, latitude: Int, longitude: Int)

case class OSBlpu(uprn: Long, parentUprn: Option[Long], logicalState: Char, blpuState: Char, subdivision: Char,
                  postcode: String, localCustodianCode: Int, latitude: String, longitude: String) extends Document {

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "logicalState" -> logicalState,
    "blpuState" -> blpuState,
    "subdivision" -> subdivision,
    "localCustodianCode" -> localCustodianCode,
    "postcode" -> postcode,
    "location" -> location) ++ parentUprn.map("parentUprn" -> _)

  def location: String = {
    s"$latitude,$longitude"
  }

  def normalise: OSBlpu = new OSBlpu(uprn, parentUprn, logicalState, blpuState, subdivision,
    Postcode.normalisePostcode(postcode), localCustodianCode, latitude, longitude)
}
