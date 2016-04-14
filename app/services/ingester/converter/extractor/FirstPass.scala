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

import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter.{OSBlpu, _}
import services.ingester.exec.Task
import uk.co.hmrc.address.osgb.DbAddress

import scala.collection.{mutable, _}


trait Pass {
  def processFile(csvIterator: Iterator[Array[String]], out: (DbAddress) => Unit)

  def sizeInfo: String
}


class FirstPass(out: (DbAddress) => Unit, task: Task) extends Pass {

  private[extractor] val blpuTable: mutable.Map[Long, Blpu] = new mutable.HashMap()
  private[extractor] val dpaTable: mutable.Set[Long] = new mutable.HashSet()
  private[extractor] val streetTable: mutable.Map[Long, Street] = new mutable.HashMap()
  //  private[extractor] val lpiLogicStatusTable: mutable.Map[Long, Byte] = new mutable.HashMap()


  def firstPass: ForwardData = {
    //TODO: move to passed in logger
    ForwardData(blpuTable, dpaTable, streetTable)
  }


  def processFile(csvIterator: Iterator[Array[String]], out: (DbAddress) => Unit) {
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
        processBlpu(csvLine)

      case OSDpa.RecordId =>
        processDpa(csvLine)

      case OSStreet.RecordId =>
        processStreet(OSStreet(csvLine))

      case OSStreetDescriptor.RecordId if OSStreetDescriptor.isEnglish(csvLine) =>
        processStreetDescriptor(OSStreetDescriptor(csvLine))

      case _ =>
    }
  }

  private def processBlpu(csvLine: Array[String]): Unit = {
    val blpu = OSBlpu(csvLine)
    if(dpaTable.contains(blpu.uprn)) dpaTable.remove(blpu.uprn)
    else blpuTable += blpu.uprn -> Blpu(blpu.postcode, blpu.logicalStatus)
  }

  private def processDpa(csvLine: Array[String]): Unit = {
    val osDpa = OSDpa(csvLine)
    out(ExportDbAddress.exportDPA(osDpa))
    if (blpuTable.contains(osDpa.uprn)) blpuTable.remove(osDpa.uprn)
    else dpaTable += osDpa.uprn
  }

  private def processStreet(street: OSStreet): Unit = {
    val existing = streetTable.get(street.usrn)
    if (existing.isDefined)
    // note that this overwrites the pre-existing entry
      streetTable += street.usrn -> Street(street.recordType, existing.get.streetDescription, existing.get.localityName, existing.get.townName)
    else
      streetTable += street.usrn -> Street(street.recordType, "", "", "")
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
    s"First pass obtained ${blpuTable.size} BLPUs, ${dpaTable.size} DPA UPRNs, ${streetTable.size} streets"
}
