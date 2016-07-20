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
    subdivision = -1,
    postalCode = 16,
    postcode = 17)

  val v2 = OSBlpuIdx(
    uprn = Uprn_Idx,
    logicalStatus = 4,
    subdivision = 14,
    postalCode = 19,
    postcode = 20)

  var idx = v1

  def isUsefulPostcode(csv: Array[String]): Boolean = {
    csv(idx.postalCode) != "N" // not a postal address
  }

  def apply(csv: Array[String]): OSBlpu ={
    val subdivision = if (idx == v1) 'J' else csv(idx.subdivision).head
    OSBlpu(csv(idx.uprn).toLong, csv(idx.logicalStatus).head, subdivision, csv(idx.postcode))
  }
}

case class OSBlpuIdx(uprn: Int, logicalStatus: Int, subdivision: Int, postalCode: Int, postcode: Int)

case class OSBlpu(uprn: Long, logicalStatus: Char, subdivision: Char, postcode: String) extends Document {

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "logicalStatus" -> logicalStatus,
    "subdivision" -> subdivision,
    "postcode" -> postcode)

  def normalise: OSBlpu = new OSBlpu(uprn, logicalStatus, subdivision, Postcode.normalisePostcode(postcode))
}
