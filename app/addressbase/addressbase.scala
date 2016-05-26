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

trait Document {
  def tupled: List[scala.Tuple2[String, Any]]
}

// These case classes represent the data model for the parts of AddressBasePremium
// (https://www.ordnancesurvey.co.uk/business-and-government/products/addressbase-products.html)
// that we use. Consult the technical reference docs for more info.

object OSCleanup {

  val Uprn_Idx = 3

  def removeUninterestingStreets(s: String): String = {
    val containedPhrases = List[String](
      "from ", "pump ", "pumping ", "mast ", "hydraulic ram", "helipad ", "across from", "fire station",
      "awaiting conversion", "ppg sta", "footway", "bridge", "pipeline", "redevelopment"
      //      " adjacent to ",
      //      " adj to ",
      //      " to east of ",
      //      " to the east of ",
      //      " to north of ",
      //      " to the north of ",
      //      " to rear of ",
      //      " to the rear of ",
      //      " to south of ",
      //      " to the south of ",
      //      " to west of ",
      //      " to the west of "
    )
    val startingPhrases = List[String](
      //      "access to ",
      //      "adjacent to ",
      //      "adj to ",
      //      "back lane from ",
      //      "back lane to ",
      //      "bus shelter ",
      //      "car park ",
      //      "drive from ",
      //      "footpath from ",
      //      "footpath next ",
      //      "footpath to ",
      //      "grass verge ",
      //      "landlords supply ",
      //      "landlord's supply ",
      //      "lane from ",
      //      "lane to ",
      //      "path leading from ",
      //      "path leading to ",
      //      "public footpath to ",
      //      "road from ",
      //      "road to ",
      //      "site supply to ",
      //      "street from ",
      //      "street to ",
      //      "supply to ",
      //      "track from ",
      //      "track to "
    )
    val sl = s.toLowerCase
    if (startingPhrases.exists(w => sl.startsWith(w)) || containedPhrases.exists(w => sl.contains(w))) ""
    else s
  }
}


object OSHeader {
  val RecordId = "10"

  val Version_Idx = 7
}


//-------------------------------------------------------------------------------------------------

object OSStreet {
  val RecordId = "11"

  val idx = OSStreetIdx(usrn = OSCleanup.Uprn_Idx, recordType = 4)

  def apply(csv: Array[String]): OSStreet = OSStreet(csv(idx.usrn).toLong, csv(idx.recordType).head)
}

case class OSStreetIdx(usrn: Int, recordType: Int)

case class OSStreet(usrn: Long, recordType: Char)


//-------------------------------------------------------------------------------------------------

object OSStreetDescriptor {
  val RecordId = "15"

  val idx = OSStreetDescriptorIdx(
    usrn = OSCleanup.Uprn_Idx,
    description = 4,
    locality = 5,
    town = 6,
    language = 8)

  def isEnglish(csv: Array[String]): Boolean = csv(idx.language) == "ENG"

  def apply(csv: Array[String]): OSStreetDescriptor = OSStreetDescriptor(
    csv(idx.usrn).toLong,
    csv(idx.description).trim,
    csv(idx.locality).trim,
    csv(idx.town).trim.intern,
    csv(idx.language).trim.intern)
}

case class OSStreetDescriptorIdx(usrn: Int, description: Int, locality: Int, town: Int, language: Int)

case class OSStreetDescriptor(usrn: Long, description: String, locality: String, town: String, language: String)


//-------------------------------------------------------------------------------------------------

object OSLpi {
  val RecordId = "24"

  val idx = OSLpiIdx(
    uprn = OSCleanup.Uprn_Idx,
    logicalStatus = 6,
    saoStartNumber = 11,
    saoStartSuffix = 12,
    saoEndNumber = 13,
    saoEndSuffix = 14,
    saoText = 15,
    paoStartNumber = 16,
    paoStartSuffix = 17,
    paoEndNumber = 18,
    paoEndSuffix = 19,
    paoText = 20,
    usrn = 21)

  def apply(csv: Array[String]): OSLpi = OSLpi(
    csv(idx.uprn).toLong,
    csv(idx.logicalStatus).head,
    csv(idx.saoStartNumber).trim,
    csv(idx.saoStartSuffix).trim,
    csv(idx.saoEndNumber).trim,
    csv(idx.saoEndSuffix).trim,
    csv(idx.saoText).trim,
    csv(idx.paoStartNumber).trim,
    csv(idx.paoStartSuffix).trim,
    csv(idx.paoEndNumber).trim,
    csv(idx.paoEndSuffix).trim,
    csv(idx.paoText).trim,
    csv(idx.usrn).toLong)
}

case class OSLpiIdx(uprn: Int,
                    logicalStatus: Int,
                    saoStartNumber: Int,
                    saoStartSuffix: Int,
                    saoEndNumber: Int,
                    saoEndSuffix: Int,
                    saoText: Int,
                    paoStartNumber: Int,
                    paoStartSuffix: Int,
                    paoEndNumber: Int,
                    paoEndSuffix: Int,
                    paoText: Int,
                    usrn: Int)

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
                 usrn: Long) {

  private def numRange(sNum: String, sSuf: String, eNum: String, eSuf: String) = {
    val start = (sNum + sSuf).trim
    val end = (eNum + eSuf).trim
    (start, end) match {
      case ("", "") => ""
      case (s, "") => s
      case ("", e) => e
      case (s, e) => s + "-" + e
    }
  }

  def primaryNumberRange: String = numRange(paoStartNumber, paoStartSuffix, paoEndNumber, paoEndSuffix)

  def secondaryNumberRange: String = numRange(saoStartNumber, saoStartSuffix, saoEndNumber, saoEndSuffix)

}

//-------------------------------------------------------------------------------------------------

object OSCsv {
  // Up to Epoch-38 is variant 1. Later epochs are variant 2.
  // this may well change - see https://jira.tools.tax.service.gov.uk/browse/TXMNT-294
  def setCsvFormat(v: Int) {
    if (v == 1) {
      OSBlpu.idx = OSBlpu.v1
      OSDpa.idx = OSDpa.v1
    } else {
      OSBlpu.idx = OSBlpu.v2
      OSDpa.idx = OSDpa.v2
    }
  }

  def setCsvFormatFor(version: String): Unit = {
    version match {
      case "1.0" => setCsvFormat(1)
      case _ => setCsvFormat(2)
    }
  }

  val RecordIdentifier_idx = 0
}
