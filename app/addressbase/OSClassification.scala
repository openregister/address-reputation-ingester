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

import uk.gov.hmrc.address.osgb.Document

object OSClassification {
  val RecordId = "32"

  val idx = OSClassificationIdx(
    uprn = 3,
    code = 5,
    scheme = 6,
    version = 7)

  def apply(csv: Array[String]): OSClassification = {
    val scheme = KnownClassificationSchemes.values.find(s => s.scheme == csv(idx.scheme) && s.version == csv(idx.version))
    OSClassification(
      csv(idx.uprn).toLong,
      csv(idx.code),
      scheme.map(_.ordinal))
  }
}

case class OSClassificationIdx(uprn: Int,
                               code: Int,
                               scheme: Int,
                               version: Int)

case class OSClassification(uprn: Long,
                            code: String,
                            scheme: Option[Int]) extends Document {

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "code" -> code,
    "scheme" -> scheme)

  def normalise: OSClassification = this
}

