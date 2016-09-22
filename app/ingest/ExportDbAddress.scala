/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package ingest

import addressbase.{OSDpa, OSLpi}
import ingest.Ingester.Street
import ingest.algorithm.Algorithm
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.Capitalisation._

private[ingest] object ExportDbAddress {

  def exportDPA(dpa: OSDpa,
                subdivision: Char,
                localCustodianCode: Option[Int]): DbAddress = {
    val id = "GB" + dpa.uprn.toString
    val line1 = normaliseAddressLine(dpa.subBuildingName + " " + dpa.buildingName)
    val line2 = normaliseAddressLine(dpa.buildingNumber + " " + dpa.dependentThoroughfareName + " " + dpa.thoroughfareName)
    val line3 = normaliseAddressLine(dpa.doubleDependentLocality + " " + dpa.dependentLocality)
    val lines = List(line1, line2, line3).filterNot(_.isEmpty)

    DbAddress(id, lines,
      Some(normaliseAddressLine(dpa.postTown)),
      dpa.postcode,
      ukHomeCountryName(subdivision),
      localCustodianCode)
  }


  def exportLPI(lpi: OSLpi, postcode: String,
                streets: java.util.Map[java.lang.Long, String],
                subdivision: Char,
                localCustodianCode: Int,
                settings: Algorithm): DbAddress = {
    val id = "GB" + lpi.uprn.toString
    val streetString = Option(streets.get(lpi.usrn)).getOrElse("X|<SUnknown>|<SUnknown>|<TUnknown>")
    val street = Street.unpack(streetString)

    val line1 = (lpi.saoText, lpi.secondaryNumberRange, lpi.paoText) match {
      case ("", "", "") => ""
      case ("", "", pt) => pt
      case ("", sn, "") => sn
      case (st, "", "") => st
      case (st, sn, "") => s"$st, $sn"
      case ("", sn, pt) => s"$sn $pt"
      case (st, "", pt) => s"$st, $pt"
      case (st, sn, pt) => s"$st, $sn $pt"
    }
    val line2 = (lpi.primaryNumberRange + " " + street.filteredDescription).trim
    val line3 = street.localityName

    val n1 = removeUninterestingStreets(line1, settings)
    val n2 = removeUninterestingStreets(line2, settings)
    val n3 = removeUninterestingStreets(line3, settings)
    val lines = List(n1, n2, n3).filterNot(_.isEmpty)

    DbAddress(id, lines,
      Some(street.townName),
      postcode,
      ukHomeCountryName(subdivision),
      Some(localCustodianCode))
  }

  private def ukHomeCountryName(subdivision: Char) = subdivision match {
    case 'S' => Some("GB-SCT")
    case 'E' => Some("GB-ENG")
    case 'W' => Some("GB-WLS")
    case 'N' => Some("GB-NIR")
    case 'L' => Some("GB-CHA")
    case 'M' => Some("GB-IOM")
    //    case 'J' => "" // unknown subdivision
    case _ => None
  }

  private def removeUninterestingStreets(s: String, settings: Algorithm): String = {
    val sl = s.toLowerCase
    if (settings.startingPhrases.exists(w => sl.startsWith(w)) || settings.containedPhrases.exists(w => sl.contains(w))) ""
    else s
  }
}
