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
import services.ingester.exec.Continuer
import services.ingester.writers.OutputWriter

import scala.collection._


trait Pass {
  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter)

  def sizeInfo: String
}


class FirstPass(out: OutputWriter, continuer: Continuer, val forwardData: ForwardData = ForwardData.chronicleInMemory()) extends Pass {

  // The simple 'normal' collections
  //  val forwardData = ForwardData.simpleInstance()

  // The enhanced Chronicle collections
//  val forwardData = ForwardData.chronicleInMemory()

  //For development only (runs slower and leaves temp files behind)
  //  val forwardData = ForwardData.chronicleWithFile()

  def firstPass: ForwardData = forwardData


  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter) {
    for (csvLine <- csvIterator
         if continuer.isBusy) {
      processLine(csvLine, out)
    }
  }


  private def processLine(csvLine: Array[String], out: OutputWriter) {

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
    forwardData.blpu.put(blpu.uprn, Blpu(blpu.postcode, blpu.logicalStatus).pack)
  }

  private def processDpa(csvLine: Array[String]): Unit = {
    val osDpa = OSDpa(csvLine)
    forwardData.dpa.add(osDpa.uprn)
  }

  private def processStreet(street: OSStreet): Unit = {
    if (forwardData.streets.containsKey(street.usrn)) {
      val existingStreetStr = forwardData.streets.get(street.usrn)
      val existingStreet = Street.unpack(existingStreetStr)
      forwardData.streets.put(street.usrn, Street(street.recordType, existingStreet.streetDescription, existingStreet.localityName, existingStreet.townName).pack)
    } else {
      forwardData.streets.put(street.usrn, Street(street.recordType, "", "", "").pack)
    }
  }

  private def processStreetDescriptor(sd: OSStreetDescriptor) {
    if (forwardData.streets.containsKey(sd.usrn)) {
      val existingStreetStr = forwardData.streets.get(sd.usrn)
      val existingStreet = Street.unpack(existingStreetStr)
      forwardData.streets.put(sd.usrn, Street(existingStreet.recordType, sd.description, sd.locality, sd.town).pack)
    } else {
      forwardData.streets.put(sd.usrn, Street('A', sd.description, sd.locality, sd.town).pack)
    }
  }

  def sizeInfo: String = s"First pass obtained ${forwardData.sizeInfo}"
}
