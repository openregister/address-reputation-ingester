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
import uk.gov.hmrc.address.uk.Postcode

object OSDpa {
  val RecordId = "28"

  // scalastyle:off
  val v1 = OSDpaIdx(
    uprn = 3,
    subBuildingName = 8,
    buildingName = 9,
    buildingNumber = 10,
    dependentThoroughfareName = 11,
    thoroughfareName = 12,
    doubleDependentLocality = 13,
    dependentLocality = 14,
    postTown = 15,
    postcode = 16)

  val v2 = OSDpaIdx(
    uprn = 3,
    subBuildingName = 7,
    buildingName = 8,
    buildingNumber = 9,
    dependentThoroughfareName = 10,
    thoroughfareName = 11,
    doubleDependentLocality = 12,
    dependentLocality = 13,
    postTown = 14,
    postcode = 15)

  var idx = v1

  def apply(csv: Array[String]): OSDpa =
    OSDpa(
      csv(idx.uprn).toLong,
      csv(idx.subBuildingName).trim,
      csv(idx.buildingName).trim,
      csv(idx.buildingNumber).trim,
      csv(idx.dependentThoroughfareName).trim,
      csv(idx.thoroughfareName).trim,
      csv(idx.doubleDependentLocality).trim,
      csv(idx.dependentLocality).trim,
      csv(idx.postTown).trim,
      csv(idx.postcode).trim)
}

case class OSDpaIdx(uprn: Int,
                    subBuildingName: Int,
                    buildingName: Int,
                    buildingNumber: Int,
                    dependentThoroughfareName: Int,
                    thoroughfareName: Int,
                    doubleDependentLocality: Int,
                    dependentLocality: Int,
                    postTown: Int,
                    postcode: Int)

case class OSDpa(uprn: Long,
                 subBuildingName: String,
                 buildingName: String,
                 buildingNumber: String,
                 dependentThoroughfareName: String,
                 thoroughfareName: String,
                 doubleDependentLocality: String,
                 dependentLocality: String,
                 postTown: String,
                 postcode: String) extends Document {

  // For use as input to MongoDbObject (hence it's not a Map)
  def tupled: List[(String, Any)] = List(
    "uprn" -> uprn,
    "subBuildingName" -> subBuildingName,
    "buildingName" -> buildingName,
    "buildingNumber" -> buildingNumber,
    "dependentThoroughfareName" -> dependentThoroughfareName,
    "thoroughfareName" -> thoroughfareName,
    "doubleDependentLocality" -> doubleDependentLocality,
    "dependentLocality" -> dependentLocality,
    "postTown" -> postTown,
    "postcode" -> postcode)

  def normalise: OSDpa = new OSDpa(uprn, subBuildingName,
    buildingName,
    buildingNumber,
    Capitalisation.normaliseAddressLine(dependentThoroughfareName),
    Capitalisation.normaliseAddressLine(thoroughfareName),
    Capitalisation.normaliseAddressLine(doubleDependentLocality),
    Capitalisation.normaliseAddressLine(dependentLocality),
    Capitalisation.normaliseAddressLine(postTown),
    Postcode.normalisePostcode(postcode))
}

