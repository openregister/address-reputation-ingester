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
    if (wordList.exists(l => s.toLowerCase.contains(l.toLowerCase))) "" else s
  }


  implicit class StringCleanup(s: String) {
    def cleanup: String = {
      val s1 = s.trim
      if (s1 == "\"\"") ""
      else {
        val s2 = if (s1.length > 0 && s1.head == '"') s1.tail.trim else s1
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
  val RecordIdentifier_idx = 0

}

object OSBlpu {
  val RecordId = "21"

  val LogicalStatus_Idx = 4
  val PostalAddrCode_Idx = 16
  val Postcode_Idx = 17

  def apply(csv: Array[String]):OSBlpu = new OSBlpu(csv.toVector)
}

class OSBlpu(csv: Vector[String]) {
  import OSCleanup._
  import OSBlpu._

  lazy val uprn = csv(Uprn_Idx).toLong

  def logicalStatus:Char = csv(LogicalStatus_Idx).head

  def postcode:String = csv(Postcode_Idx)
}


object OSDpa {
  val RecordId = "28"

  val SubBuildingName_Idx = 8
  val BuildingName_Idx = 9
  val BuildingNumber_Idx = 10
  val DependentThoroughfareName_Idx = 11
  val ThoroughfareName_Idx = 12
  val DoubleDependentLocality_Idx = 13
  val DependentLocality_Idx = 14
  val PostTown_Idx = 15
  val Postcode_Idx = 16

  def apply(csv: Array[String]):OSDpa = new OSDpa(csv.toVector)
}


class OSDpa(csv: Vector[String]) {

  import OSCleanup._
  import OSDpa._

  lazy val uprn:Long = csv(Uprn_Idx).toLong

  def subBuildingName:String = csv(SubBuildingName_Idx).cleanup

  def buildingName:String = csv(BuildingName_Idx).cleanup

  def buildingNumber:String = csv(BuildingNumber_Idx).cleanup

  def dependentThoroughfareName:String = csv(DependentThoroughfareName_Idx).cleanup

  def thoroughfareName:String = csv(ThoroughfareName_Idx).cleanup

  def doubleDependentLocality:String = csv(DoubleDependentLocality_Idx).cleanup

  def dependentLocality:String = csv(DependentLocality_Idx).cleanup

  def postTown:String = csv(PostTown_Idx).cleanup

  def postcode:String = csv(Postcode_Idx).cleanup

}


object OSStreet {
  val RecordId = "11"

  val RecordType_Idx = 4

  def apply(csv: Array[String]):OSStreet = new OSStreet(csv.toVector)
}

class OSStreet(csv: Vector[String]) {
  import OSCleanup._
  import OSStreet._

  lazy val usrn:Long = csv(Uprn_Idx).toLong

  def recordType:Char = csv(RecordType_Idx).head
}

object OSStreetDescriptor {
  val RecordId = "15"
  val Language_Idx = 8

  val Description_Idx = 4
  val Locality_Idx = 5
  val Town_Idx = 6


  def apply(csv: Array[String]):OSStreetDescriptor = new OSStreetDescriptor(csv.toVector)
}

class OSStreetDescriptor(csv: Vector[String]) {

  import OSCleanup._
  import OSStreetDescriptor._

  lazy val usrn:Long = csv(Uprn_Idx).toLong

  def description: String = csv(Description_Idx).cleanup

  def locality: String = csv(Locality_Idx).cleanup

  def town: String = csv(Town_Idx).cleanup.intern
}


object OSLpi {
  val RecordId = "24"

  val TogicalStatus_Idx = 6
  val SaoStartNumber_Idx = 11
  val SaoStartSuffix_Idx = 12
  val SaoEndNumber_Idx = 13
  val SaoEndSuffix_Idx = 14
  val SaoText_Idx = 15
  val PaoStartNumber_Idx = 16
  val PaoStartSuffix_Idx = 17
  val PaoEndNumber_Idx = 18
  val PaoEndSuffix_Idx = 19
  val PaoText_Idx = 20
  val Usrn_Idx = 21


  def apply(csv: Array[String]): OSLpi = new OSLpi(csv.toVector)
}

class OSLpi(csv: Vector[String]) {

  import OSCleanup._
  import OSLpi._

  lazy val uprn:Long = csv(Uprn_Idx).toLong

  def logicalStatus:Char = csv(TogicalStatus_Idx).head

  def saoStartNumber:String = csv(SaoStartNumber_Idx).cleanup

  def saoStartSuffix:String = csv(SaoStartSuffix_Idx).cleanup

  def saoEndNumber:String = csv(SaoEndNumber_Idx).cleanup

  def saoEndSuffix:String = csv(SaoEndSuffix_Idx).cleanup

  def saoText:String = csv(SaoText_Idx).cleanup

  def paoStartNumber:String = csv(PaoStartNumber_Idx).cleanup

  def paoStartSuffix:String = csv(PaoStartSuffix_Idx).cleanup

  def paoEndNumber:String = csv(PaoEndNumber_Idx).cleanup

  def paoEndSuffix:String = csv(PaoEndSuffix_Idx).cleanup

  def paoText:String = csv(PaoText_Idx).cleanup

  def usrn:Long = csv(Usrn_Idx).toLong
}

