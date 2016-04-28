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

package services.ingester.converter.extractor

import services.ingester.converter.Extractor.Street
import services.ingester.converter.{OSCleanup, OSDpa, OSLpi}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.osgb.Postcode._
import uk.co.hmrc.address.services.Capitalisation._

private[extractor] object ExportDbAddress {

  def exportDPA(dpa: OSDpa): DbAddress = {
    val id = "GB" + dpa.uprn.toString
    val line1 = normaliseAddressLine(dpa.subBuildingName + " " + dpa.buildingName)
    val line2 = normaliseAddressLine(dpa.buildingNumber + " " + dpa.dependentThoroughfareName + " " + dpa.thoroughfareName)
    val line3 = normaliseAddressLine(dpa.doubleDependentLocality + " " + dpa.dependentLocality)

    DbAddress(id, line1, line2, line3, normaliseAddressLine(dpa.postTown), normalisePostcode(dpa.postcode))
  }


  def exportLPI(lpi: OSLpi, postcode: String, streets: java.util.Map[java.lang.Long, String]): DbAddress = {
    val streetString = if (streets.containsKey(lpi.usrn)) streets.get(lpi.usrn) else "X|<SUnknown>|<SUnknown>|<TUnknown>"
    val street = Street.unpack(streetString)

    val line1 = (lpi.saoText + " " + lpi.secondaryNumberRange + " " + lpi.paoText).trim

    val line2 = (lpi.primaryNumberRange + " " + street.filteredDescription).trim

    val line3 = street.localityName

    DbAddress(
      "GB" + lpi.uprn.toString,
      normaliseAddressLine(OSCleanup.removeUninterestingStreets(line1)),
      normaliseAddressLine(OSCleanup.removeUninterestingStreets(line2)),
      normaliseAddressLine(OSCleanup.removeUninterestingStreets(line3)),
      normaliseAddressLine(street.townName),
      normalisePostcode(postcode))
  }
}
