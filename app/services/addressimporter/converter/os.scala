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

package services.addressimporter.converter

object OSCleanup {

  val Uprn_Idx = 3

  def removeBannedStreets(s: String): String = {
    val wordList = List[String](
      "From ", "Pump ", "Pumping ", "Mast ", "Hydraulic Ram", "Helipad ", "Across From", "Fire Station",
      "Awaiting Conversion", "Ppg Sta", "Footway", "Bridge", "Pipeline", "Redevelopment"
    )
    if (wordList.exists(w => s.toLowerCase.contains(w.toLowerCase))) "" else s
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

    def capitalisation: String = {
      Capitalisation.normaliseAddressLine(s)
    }
  }

}


object OSCsv {
  var csvFormat = 2

  val RecordIdentifier_idx = 0
}


object OSHeader {
  val RecordId = "10"

  val Version_Idx = 7
}


object OSBlpu {
  val RecordId = "21"

  def isSmallPostcode(csv: Array[String]): Boolean = {
    def postalAddrCodeIdx:Int = if (OSCsv.csvFormat == 1) 16 else 19

    csv(postalAddrCodeIdx) == "S"
  }

  import OSCleanup._

  def apply(csv: Array[String]): OSBlpu =
    if (OSCsv.csvFormat == 1)
      new OSBlpu(csv(Uprn_Idx).toLong, csv(4).head, csv(17))
    else
      new OSBlpu(csv(Uprn_Idx).toLong, csv(4).head, csv(20))
}

case class OSBlpu(uprn: Long,
                  logicalStatus: Char,
                  postcode: String)


object OSDpa {
  val RecordId = "28"

  import OSCleanup._

  def apply(csv: Array[String]): OSDpa =
    if (OSCsv.csvFormat == 1)
      new OSDpa(
        csv(Uprn_Idx).toLong,
        csv(8).cleanup,
        csv(9).cleanup,
        csv(10).cleanup,
        csv(11).cleanup,
        csv(12).cleanup,
        csv(13).cleanup,
        csv(14).cleanup,
        csv(15).cleanup,
        csv(16).cleanup)
    else
      new OSDpa(
        csv(Uprn_Idx).toLong,
        csv(7).cleanup,
        csv(8).cleanup,
        csv(9).cleanup,
        csv(10).cleanup,
        csv(11).cleanup,
        csv(12).cleanup,
        csv(13).cleanup,
        csv(14).cleanup,
        csv(15).cleanup)
}

case class OSDpa(uprn: Long,
                 subBuildingName: String,
                 buildingName: String,
                 buildingNumber: String,
                 dependentThoroughfareName: String,
                 thoroughfareName: String,
                 doubleDependentLocality: String,
                 dependentLocality: String,
                 postTown: String,
                 postcode: String)


object OSStreet {
  val RecordId = "11"

  import OSCleanup._

  def apply(csv: Array[String]): OSStreet = new OSStreet(csv(Uprn_Idx).toLong, csv(4).head)
}

case class OSStreet(usrn: Long,
                    recordType: Char)


object OSStreetDescriptor {
  val RecordId = "15"

  def isEnglish(csv: Array[String]): Boolean = csv(8) == "ENG"

  import OSCleanup._

  def apply(csv: Array[String]): OSStreetDescriptor = new OSStreetDescriptor(
    csv(Uprn_Idx).toLong,
    csv(4).cleanup,
    csv(5).cleanup,
    csv(6).cleanup.intern,
    csv(8).cleanup.intern)
}

case class OSStreetDescriptor(usrn: Long,
                              description: String,
                              locality: String,
                              town: String,
                              language: String)


object OSLpi {
  val RecordId = "24"

  import OSCleanup._

  def apply(csv: Array[String]): OSLpi = new OSLpi(
    csv(Uprn_Idx).toLong,
    csv(6).head,
    csv(11).cleanup,
    csv(12).cleanup,
    csv(13).cleanup,
    csv(14).cleanup,
    csv(15).cleanup,
    csv(16).cleanup,
    csv(17).cleanup,
    csv(18).cleanup,
    csv(19).cleanup,
    csv(20).cleanup,
    csv(21).toLong)
}

case class OSLpi(uprn: Long,
                 logicalStatus: Char,
                 saoStartNumber: String,
                 saoStartSuffix: String,
                 saoEndNumber: String,
                 saoEndSuffix: String,
                 saoText: String,
                 paoStartNumber: String,
                 paoStartSuffix: String,
                 paoEndNumber: String,
                 paoEndSuffix: String,
                 paoText: String,
                 usrn: Long)
