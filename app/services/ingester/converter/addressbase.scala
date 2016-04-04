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

package services.ingester.converter

// These case classes represent the data model for the parts of AddressBasePremium
// (https://www.ordnancesurvey.co.uk/business-and-government/products/addressbase-products.html)
// that we use. Consult the technical reference docs for more info.

object OSCleanup {

  val Uprn_Idx = 3

  def removeUninterestingStreets(s: String): String = {
    val wordList = List[String](
      "from ", "pump ", "pumping ", "mast ", "hydraulic ram", "helipad ", "across from", "fire station",
      "awaiting conversion", "ppg sta", "footway", "bridge", "pipeline", "redevelopment"
    )
    if (wordList.exists(w => s.toLowerCase.contains(w))) "" else s
  }


  implicit class StringCleanup(s: String) {
    def cleanup: String = {
      val s1 = s.trim
      if (s1 == "\"\"") ""
      else {
        val s2 = if (s1.nonEmpty && s1.head == '"') s1.tail.trim else s1
        if (s2.nonEmpty && s2.last == '"') s2.init.trim else s2
      }
    }

    def rmDupSpace: String = {
      s.replaceAll("  ", " ")
    }
  }

}


object OSCsv {
  // Up to Epoch-38 is variant 1. Later epochs are variant 2.
  var csvFormat = 2

  val RecordIdentifier_idx = 0
}


object OSHeader {
  val RecordId = "10"

  val Version_Idx = 7
}


object OSBlpu {
  val RecordId = "21"

  val V1LogicalStatusIdx = 4
  val V1PostalAddrCodeIdx = 16
  val V1PostcodeIdx = 17

  val V2LogicalStatusIdx = 4
  val V2PostalAddrCodeIdx = 19
  val V2PostcodeIdx = 20


  def isSmallPostcode(csv: Array[String]): Boolean = {
    def postalAddrCodeIdx: Int = if (OSCsv.csvFormat == 1) V1PostalAddrCodeIdx else V2PostalAddrCodeIdx

    csv(postalAddrCodeIdx) == "S"
  }

  import OSCleanup._

  def apply(csv: Array[String]): OSBlpu =
    if (OSCsv.csvFormat == 1)
      OSBlpu(csv(Uprn_Idx).toLong, csv(V1LogicalStatusIdx).head, csv(V1PostcodeIdx))
    else
      OSBlpu(csv(Uprn_Idx).toLong, csv(V2LogicalStatusIdx).head, csv(V2PostcodeIdx))
}

case class OSBlpu(uprn: Long, logicalStatus: Char, postcode: String)


object OSDpa {
  val RecordId = "28"

  import OSCleanup._

  val V1SubBuildingNameIdx = 8
  val V1BuildingNameIdx = 9
  val V1BuildingNumberIdx = 10
  val V1DependentThoroughfareNameIdx = 11
  val V1ThoroughfareNameIdx = 12
  val V1DoubleDependentLocalityIdx = 13
  val V1DependentLocalityIdx = 14
  val V1PostTownIdx = 15
  val V1PostcodeIdx = 16

  val V2SubBuildingNameIdx = 7
  val V2BuildingNameIdx = 8
  val V2BuildingNumberIdx = 9
  val V2DependentThoroughfareNameIdx = 10
  val V2ThoroughfareNameIdx = 11
  val V2DoubleDependentLocalityIdx = 12
  val V2DependentLocalityIdx = 13
  val V2PostTownIdx = 14
  val V2PostcodeIdx = 15


  def apply(csv: Array[String]): OSDpa =
    if (OSCsv.csvFormat == 1)
      OSDpa(
        csv(Uprn_Idx).toLong,
        csv(V1SubBuildingNameIdx).cleanup,
        csv(V1BuildingNameIdx).cleanup,
        csv(V1BuildingNumberIdx).cleanup,
        csv(V1DependentThoroughfareNameIdx).cleanup,
        csv(V1ThoroughfareNameIdx).cleanup,
        csv(V1DoubleDependentLocalityIdx).cleanup,
        csv(V1DependentLocalityIdx).cleanup,
        csv(V1PostTownIdx).cleanup,
        csv(V1PostcodeIdx).cleanup)
    else
      OSDpa(
        csv(Uprn_Idx).toLong,
        csv(V2SubBuildingNameIdx).cleanup,
        csv(V2BuildingNameIdx).cleanup,
        csv(V2BuildingNumberIdx).cleanup,
        csv(V2DependentThoroughfareNameIdx).cleanup,
        csv(V2ThoroughfareNameIdx).cleanup,
        csv(V2DoubleDependentLocalityIdx).cleanup,
        csv(V2DependentLocalityIdx).cleanup,
        csv(V2PostTownIdx).cleanup,
        csv(V2PostcodeIdx).cleanup)
}

case class OSDpa(uprn: Long, subBuildingName: String, buildingName: String, buildingNumber: String,
                 dependentThoroughfareName: String, thoroughfareName: String, doubleDependentLocality: String,
                 dependentLocality: String, postTown: String, postcode: String)


object OSStreet {
  val RecordId = "11"

  import OSCleanup._

  val RecordTypeIdx = 4

  def apply(csv: Array[String]): OSStreet = OSStreet(csv(Uprn_Idx).toLong, csv(RecordTypeIdx).head)
}

case class OSStreet(usrn: Long, recordType: Char)


object OSStreetDescriptor {
  val RecordId = "15"

  import OSCleanup._

  val DescriptionIdx = 4
  val LocalityIdx = 5
  val TownIdx = 6
  val LanguageIdx = 8

  def isEnglish(csv: Array[String]): Boolean = csv(LanguageIdx) == "ENG"

  def apply(csv: Array[String]): OSStreetDescriptor = OSStreetDescriptor(
    csv(Uprn_Idx).toLong,
    csv(DescriptionIdx).cleanup,
    csv(LocalityIdx).cleanup,
    csv(TownIdx).cleanup.intern,
    csv(LanguageIdx).cleanup.intern)
}

case class OSStreetDescriptor(usrn: Long, description: String, locality: String, town: String, language: String)


object OSLpi {
  val RecordId = "24"

  import OSCleanup._

  val LogicalStatusIdx = 6
  val SaoStartNumberIdx = 11
  val SaoStartSuffixIdx = 12
  val SaoEndNumberIdx = 13
  val SaoEndSuffixIdx = 14
  val SaoTextIdx = 15
  val PaoStartNumberIdx = 16
  val PaoStartSuffixIdx = 17
  val PaoEndNumberIdx = 18
  val PaoEndSuffixIdx = 19
  val PaoTextIdx = 20
  val UsrnIdx = 21


  def apply(csv: Array[String]): OSLpi = OSLpi(
    csv(Uprn_Idx).toLong,
    csv(LogicalStatusIdx).head,
    csv(SaoStartNumberIdx).cleanup,
    csv(SaoStartSuffixIdx).cleanup,
    csv(SaoEndNumberIdx).cleanup,
    csv(SaoEndSuffixIdx).cleanup,
    csv(SaoTextIdx).cleanup,
    csv(PaoStartNumberIdx).cleanup,
    csv(PaoStartSuffixIdx).cleanup,
    csv(PaoEndNumberIdx).cleanup,
    csv(PaoEndSuffixIdx).cleanup,
    csv(PaoTextIdx).cleanup,
    csv(UsrnIdx).toLong)
}

case class OSLpi(uprn: Long, logicalStatus: Char, saoStartNumber: String, saoStartSuffix: String,
                 saoEndNumber: String, saoEndSuffix: String, saoText: String,
                 paoStartNumber: String, paoStartSuffix: String, paoEndNumber: String,
                 paoEndSuffix: String, paoText: String, usrn: Long)
