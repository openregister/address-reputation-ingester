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
import uk.gov.hmrc.address.services.Capitalisation

object OSStreet {
  val RecordId = "11"

  val idx = OSStreetIdx(usrn = 3, recordType = 4, classification = 9)

  def apply(csv: Array[String]): OSStreet =
    OSStreet(
      csv(idx.usrn).toLong,
      csv(idx.recordType).head,
      csv(idx.classification)
    )
}

case class OSStreetIdx(usrn: Int, recordType: Int, classification: Int)

case class OSStreet(usrn: Long, recordType: Char, classification: String) extends Document {
  def tupled: List[(String, AnyVal)] = List(
    "usrn" -> usrn,
    "recordType" -> recordType)

  def normalise: OSStreet = this
}


//-------------------------------------------------------------------------------------------------

object OSStreetDescriptor {
  val RecordId = "15"

  val idx = OSStreetDescriptorIdx(
    usrn = 3,
    description = 4,
    locality = 5,
    town = 6,
    language = 8)

  def apply(csv: Array[String]): OSStreetDescriptor = OSStreetDescriptor(
    csv(idx.usrn).toLong,
    csv(idx.description).trim,
    csv(idx.locality).trim,
    csv(idx.town).trim.intern,
    csv(idx.language).trim.intern)
}

case class OSStreetDescriptorIdx(usrn: Int, description: Int, locality: Int, town: Int, language: Int)

case class OSStreetDescriptor(usrn: Long, description: String, locality: String, town: String, language: String) extends Document {
  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "usrn" -> usrn,
    "description" -> description,
    "locality" -> locality,
    "town" -> town,
    "language" -> language)

  def normalise: OSStreetDescriptor = new OSStreetDescriptor(usrn,
    Capitalisation.normaliseAddressLine(description),
    Capitalisation.normaliseAddressLine(locality),
    Capitalisation.normaliseAddressLine(town),
    language)
}


