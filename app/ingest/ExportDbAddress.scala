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
import ingest.Ingester.{Blpu, Street, StreetDescriptor}
import ingest.algorithm.Algorithm
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.Capitalisation._

private[ingest] object ExportDbAddress {
  val GBPrefix = "GB"

  def exportDPA(dpa: OSDpa,
                blpu: Blpu,
                language: String): DbAddress = {
    val id = GBPrefix + dpa.uprn.toString
    val line1 = List(normaliseAddressLine(dpa.subBuildingName), normaliseAddressLine(dpa.buildingName)).filterNot(_.isEmpty).mkString(", ")
    val line2 = normaliseAddressLine(dpa.buildingNumber + " " + dpa.dependentThoroughfareName + " " + dpa.thoroughfareName)
    val line3 = normaliseAddressLine(dpa.doubleDependentLocality + " " + dpa.dependentLocality)
    val lines = List(line1, line2, line3).filterNot(_.isEmpty)

    DbAddress(id, lines,
      Some(normaliseAddressLine(dpa.postTown)),
      dpa.postcode,
      subdivisionCode(blpu.subdivision),
      countryCode(blpu.subdivision, dpa.postcode),
      Some(blpu.localCustodianCode),
      Some(language),
      toInt(blpu.blpuState),
      toInt(blpu.logicalState),
      None)
  }

  def exportLPI(lpi: OSLpi,
                blpu: Blpu,
                street: Street,
                streetDescriptor: StreetDescriptor,
                settings: Algorithm): DbAddress = {
    val id = GBPrefix + lpi.uprn.toString

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

    val filteredDescription =
      if (street.recordType == Ingester.StreetTypeOfficialDesignatedName) streetDescriptor.streetDescription
      else ""

    val line2 = (lpi.primaryNumberRange + " " + filteredDescription).trim
    val line3 = streetDescriptor.localityName

    val n1 = removeUninterestingStreets(line1, settings)
    val n2 = removeUninterestingStreets(line2, settings)
    val n3 = removeUninterestingStreets(line3, settings)
    val lines = List(n1, n2, n3).filterNot(_.isEmpty)

    DbAddress(id, lines,
      Some(streetDescriptor.townName),
      blpu.postcode,
      subdivisionCode(blpu.subdivision),
      countryCode(blpu.subdivision, blpu.postcode),
      Some(blpu.localCustodianCode),
      isoLanguage(lpi.language),
      toInt(blpu.blpuState),
      toInt(blpu.logicalState),
      toInt(street.classification))
  }

  private def subdivisionCode(subdivision: Char) = subdivision match {
    case 'S' => Some("GB-SCT")
    case 'E' => Some("GB-ENG")
    case 'W' => Some("GB-WLS")
    case 'N' => Some("GB-NIR")
    //    case 'L' -- not in UK
    //    case 'M' -- not in UK
    //    case 'J' => "" // unknown subdivision
    case _ => None
  }

  private def countryCode(subdivision: Char, postcode: String) = subdivision match {
    case 'S' => Some("UK")
    case 'E' => Some("UK")
    case 'W' => Some("UK")
    case 'N' => Some("UK")
    case 'L' if postcode.startsWith("G") => Some("GG")
    case 'L' if postcode.startsWith("J") => Some("JE")
    case 'M' => Some("IM")
    //    case 'J' => "" // unknown subdivision
    case _ => None
  }

  private def removeUninterestingStreets(s: String, settings: Algorithm): String = {
    val sl = s.toLowerCase
    if (settings.startingPhrases.exists(w => sl.startsWith(w)) || settings.containedPhrases.exists(w => sl.contains(w))) ""
    else s
  }

  private def isoLanguage(lang: String): Option[String] =
    lang.toLowerCase match {
      case "eng" => Some("en")
      case "cym" => Some("cy")
      case _ => None
    }

  private def toInt(c: Char) =
    if ('0' <= c && c <= '9') Some(c - '0')
    else None

  private def toInt(s: String) =
    if (s.isEmpty) None else Some(s.toInt)
}
