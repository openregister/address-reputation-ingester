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

import uk.co.hmrc.address.osgb.{Document, Postcode}

object OSBlpu {
  val RecordId = "21"

  import OSCleanup._

  // scalastyle:off
  val v1 = OSBlpuIdx(
    uprn = Uprn_Idx,
    logicalStatus = 4,
    postalCode = 16,
    postcode = 17)

  val v2 = OSBlpuIdx(
    uprn = Uprn_Idx,
    logicalStatus = 4,
    postalCode = 19,
    postcode = 20)

  var idx = v1

  def isUsefulPostcode(csv: Array[String]): Boolean = {
    csv(idx.postalCode) != "N" // not a postal address
  }

  def apply(csv: Array[String]): OSBlpu =
    OSBlpu(csv(idx.uprn).toLong, csv(idx.logicalStatus).head, csv(idx.postcode))
}

case class OSBlpuIdx(uprn: Int, logicalStatus: Int, postalCode: Int, postcode: Int)

case class OSBlpu(uprn: Long, logicalStatus: Char, postcode: String) extends Document {

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "logicalStatus" -> logicalStatus,
    "postcode" -> postcode)

  def normalise: OSBlpu = new OSBlpu(uprn, logicalStatus, Postcode.normalisePostcode(postcode))
}
