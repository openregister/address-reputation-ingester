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

import java.io.File

import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.osgb.Postcode._
import uk.co.hmrc.address.services.Capitalisation._

import scala.collection.{mutable, _}

class FirstPass(files: Seq[File], out: (DbAddress) => Unit, dt: DiagnosticTimer) {

  private[extractor] val blpuTable: mutable.Map[Long, Blpu] = new mutable.HashMap()
  private[extractor] val dpaTable: mutable.Set[Long] = new mutable.HashSet()
  private[extractor] val streetTable: mutable.Map[Long, Street] = new mutable.HashMap()
  //  private[extractor] val lpiLogicStatusTable: mutable.Map[Long, Byte] = new mutable.HashMap()


  def firstPass: ForwardData = {
    for (file <- files) {
      LoadZip.zipReader(file, dt)(processFile(_, out))
      println(sizeInfo)
    }
    ForwardData(blpuTable, dpaTable, streetTable)
  }


  def exportDPA(dpa: OSDpa)(out: (DbAddress) => Unit): Unit = {
    val id = "GB" + dpa.uprn.toString
    val line1 = normaliseAddressLine(dpa.subBuildingName + " " + dpa.buildingName)
    val line2 = normaliseAddressLine(dpa.buildingNumber + " " + dpa.dependentThoroughfareName + " " + dpa.thoroughfareName)
    val line3 = normaliseAddressLine(dpa.doubleDependentLocality + " " + dpa.dependentLocality)

    out(DbAddress(id, line1, line2, line3, normaliseAddressLine(dpa.postTown), normalisePostcode(dpa.postcode)))
  }


  private[extractor] def processFile(csvIterator: Iterator[Array[String]], out: (DbAddress) => Unit) {
    for (csvLine <- csvIterator) {
      processLine(csvLine, out)
    }
  }


  private def processLine(csvLine: Array[String], out: (DbAddress) => Unit) {

    csvLine(OSCsv.RecordIdentifier_idx) match {
      case OSHeader.RecordId =>
        if (csvLine(OSHeader.Version_Idx) == "1.0")
          OSCsv.setCsvFormat(1)
        else
          OSCsv.setCsvFormat(2)

      case OSBlpu.RecordId if OSBlpu.isUsefulPostcode(csvLine) =>
        val blpu = OSBlpu(csvLine)
        blpuTable += blpu.uprn -> Blpu(blpu.postcode, blpu.logicalStatus)

      case OSDpa.RecordId =>
        val osDpa = OSDpa(csvLine)
        exportDPA(osDpa)(out)
        dpaTable += osDpa.uprn

      case OSStreet.RecordId =>
        processStreet(OSStreet(csvLine))

      case OSStreetDescriptor.RecordId if OSStreetDescriptor.isEnglish(csvLine) =>
        processStreetDescriptor(OSStreetDescriptor(csvLine))

      case _ =>
    }
  }

  private def processStreet(street: OSStreet): Unit = {
    val existing = streetTable.get(street.usrn)
    if (existing.isDefined)
    // note that this overwrites the pre-existing entry
      streetTable += street.usrn -> Street(street.recordType, existing.get.streetDescription, existing.get.localityName, existing.get.townName)
    else
      streetTable += street.usrn -> Street(street.recordType)
  }

  private def processStreetDescriptor(sd: OSStreetDescriptor) {
    val existing = streetTable.get(sd.usrn)
    if (existing.isDefined)
    // note that this overwrites the pre-existing entry
      streetTable += sd.usrn -> Street(existing.get.recordType, sd.description, sd.locality, sd.town)
    else
      streetTable += sd.usrn -> Street('A', sd.description, sd.locality, sd.town)
  }

  def sizeInfo: String =
    s"FirstPass contains ${blpuTable.size} BLPUs, ${dpaTable.size} DPA UPRNs, ${streetTable.size} streets"
}
