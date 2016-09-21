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

import addressbase._
import ingest.Ingester._
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import services.exec.Continuer

import scala.collection._


trait Pass {
  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter): Boolean

  def sizeInfo: String
}


class FirstPass(out: OutputWriter, continuer: Continuer, settings: Algorithm, val forwardData: ForwardData) extends Pass {

  // The simple 'normal' collections
  //  val forwardData = ForwardData.simpleInstance()

  // The enhanced Chronicle collections
  //  val forwardData = ForwardData.chronicleInMemory()

  //For development only (runs slower and leaves temp files behind)
  //  val forwardData = ForwardData.chronicleWithFile()

  def firstPass: ForwardData = forwardData

  // scalastyle:off
  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter): Boolean = {
    var needSecondPass = false

    for (csvLine <- csvIterator
         if continuer.isBusy) {

      csvLine(OSCsv.RecordIdentifier_idx) match {
        case OSHeader.RecordId =>
          OSCsv.setCsvFormatFor(csvLine(OSHeader.Version_Idx))

        case OSBlpu.RecordId if OSBlpu.isUsefulPostcode(csvLine) =>
          processBlpu(OSBlpu(csvLine))

        case OSLpi.RecordId if settings.includeLpi =>
          if (settings.prefer == "LPI")
            processLpi(OSLpi(csvLine))
          needSecondPass = true

        case OSDpa.RecordId if settings.includeDpa =>
          if (settings.prefer == "DPA")
            processDpa(OSDpa(csvLine))
          needSecondPass = true

        case OSStreet.RecordId =>
          processStreet(OSStreet(csvLine))

        case OSStreetDescriptor.RecordId if OSStreetDescriptor.isEnglish(csvLine) =>
          processStreetDescriptor(OSStreetDescriptor(csvLine))

        case _ =>
      }
    }

    needSecondPass
  }

  private def processBlpu(osBlpu: OSBlpu) {
    val n = osBlpu.normalise
    val blpu = Blpu(n.postcode, n.logicalStatus, n.subdivision, Some(n.localCustodianCode))
    forwardData.blpu.put(osBlpu.uprn, blpu.pack)
  }

  private def processLpi(osLpi: OSLpi) {
    forwardData.uprns.add(osLpi.uprn)
  }

  private def processDpa(osDpa: OSDpa) {
    forwardData.uprns.add(osDpa.uprn)
  }

  // TODO this code could be simplified by storing street and descriptor separately, then joining them
  // in the second pass instead.

  private def processStreet(osStreet: OSStreet) {
    val existingStreetStr = Option(forwardData.streets.get(osStreet.usrn))
    if (existingStreetStr.isDefined) {
      val existingStreet = Street.unpack(existingStreetStr.get)
      val street = Street(osStreet.recordType, existingStreet.streetDescription, existingStreet.localityName, existingStreet.townName)
      forwardData.streets.put(osStreet.usrn, street.pack)
    } else {
      val street = Street(osStreet.recordType, "", "", "")
      forwardData.streets.put(osStreet.usrn, street.pack)
    }
  }

  private def processStreetDescriptor(xsd: OSStreetDescriptor) {
    val sd = xsd.normalise
    val existingStreetStr = Option(forwardData.streets.get(sd.usrn))
    if (existingStreetStr.isDefined) {
      val existingStreet = Street.unpack(existingStreetStr.get)
      val street = Street(existingStreet.recordType, sd.description, sd.locality, sd.town)
      forwardData.streets.put(sd.usrn, street.pack)
    } else {
      val street = Street(StreetTypeNotYetKnown, sd.description, sd.locality, sd.town)
      forwardData.streets.put(sd.usrn, street.pack)
    }
  }

  def sizeInfo: String = s"First pass obtained ${forwardData.sizeInfo}."
}
