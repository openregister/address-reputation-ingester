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

import scala.collection._

object FirstPass {

  type CSVOutput = (DbAddress) => Unit

  def firstPass(files: Vector[File], out: CSVOutput, dt: DiagnosticTimer): ForwardData = {
    def findData(f: File, fd: ForwardData): ForwardData =
      LoadZip.zipReader(f, dt)(processFile(_, fd.streets, fd.lpiLogicStatus, out))

    files.foldLeft(ForwardData.empty) {
      case (accFD, f) =>
        accFD ++ findData(f, accFD)
    }
  }


  def exportDPA(dpa: OSDpa)(out: CSVOutput): Unit = {
    val id = "GB" + dpa.uprn.toString
    val line1 = (dpa.subBuildingName + " " + dpa.buildingName).trim
    val line2 = (dpa.buildingNumber + " " + dpa.dependentThoroughfareName + " " + dpa.thoroughfareName).trim
    val line3 = (dpa.doubleDependentLocality + " " + dpa.dependentLocality).trim

    out(DbAddress(id, line1, line2, line3, dpa.postTown, dpa.postcode))
  }


  private[extractor] def processFile(csvIterator: Iterator[Array[String]], streetsMap: Map[Long, Street],
                                     lpiLogicStatusMap: Map[Long, Byte], out: CSVOutput): ForwardData = {

    csvIterator.foldLeft(new ForwardData(streets = new mutable.HashMap() ++ streetsMap, lpiLogicStatus = new mutable.HashMap() ++ lpiLogicStatusMap)) {
      case (fd, csvLine) => processLine(fd, csvLine, out)
    }
  }


  private def processLine(fd: ForwardData, csvLine: Array[String], out: CSVOutput): ForwardData =
    csvLine(OSCsv.RecordIdentifier_idx) match {

      case OSHeader.RecordId =>
        OSCsv.csvFormat = if (csvLine(OSHeader.Version_Idx) == "1.0") 1 else 2
        fd // no change

      case OSBlpu.RecordId if OSBlpu.isSmallPostcode(csvLine) =>
        val blpu = OSBlpu(csvLine)
        fd.addBlpu(blpu.uprn -> Blpu(blpu.postcode, blpu.logicalStatus))

      case OSDpa.RecordId =>
        val osDpa = OSDpa(csvLine)
        exportDPA(osDpa)(out)
        fd.addDpa(osDpa.uprn)

      case OSStreet.RecordId =>
        val street = OSStreet(csvLine)

        def updatedStreet(): Street = fd.streets.get(street.usrn).fold(Street(street.recordType)) {
          aStreet: Street =>
            Street(street.recordType, aStreet.streetDescription, aStreet.localityName, aStreet.townName)
        }

        fd.addStreet(street.usrn -> updatedStreet)

      case OSStreetDescriptor.RecordId if OSStreetDescriptor.isEnglish(csvLine) =>
        val sd = OSStreetDescriptor(csvLine)

        def updateStreet(): Street = fd.streets.get(sd.usrn).fold(
          Street('A', sd.description, sd.locality, sd.town)) {
          aStreet: Street =>
            Street(aStreet.recordType, sd.description, sd.locality, sd.town)
        }

        fd.addStreet(sd.usrn -> updateStreet)

      case _ => fd
    }

}
