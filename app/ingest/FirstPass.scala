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

        case OSStreetDescriptor.RecordId =>
          processStreetDescriptor(OSStreetDescriptor(csvLine))

        case _ =>
      }
    }

    needSecondPass
  }

  private def processBlpu(osBlpu: OSBlpu) {
    val n = osBlpu.normalise
    val blpu = Blpu(n.parentUprn, n.postcode, n.logicalState, n.blpuState, n.subdivision, n.localCustodianCode)
    forwardData.blpu.put(osBlpu.uprn, blpu.pack)

    if (n.localCustodianCode != Ingester.DefaultLCC) {
      val p = PostcodeLCC(Some(n.localCustodianCode)).pack
      if (forwardData.postcodeLCCs.containsKey(n.postcode)) {
        val existing = forwardData.postcodeLCCs.get(n.postcode)
        if (existing != p) {
          forwardData.postcodeLCCs.put(n.postcode, PostcodeLCC.Plural)
        }
      } else {
        forwardData.postcodeLCCs.put(n.postcode, p)
      }
    }
  }

  private def processLpi(osLpi: OSLpi) {
    forwardData.uprns.add(osLpi.uprn)
  }

  private def processDpa(osDpa: OSDpa) {
    forwardData.uprns.add(osDpa.uprn)
  }

  private def processStreet(osStreet: OSStreet) {
    val street = Street(osStreet.recordType, osStreet.classification)
    forwardData.streets.put(osStreet.usrn, street.pack)
  }

  private def processStreetDescriptor(xsd: OSStreetDescriptor) {
    val sd = xsd.normalise
    val street = StreetDescriptor(sd.description, sd.locality, sd.town)
    sd.language match {
      case "ENG" => forwardData.streetDescriptorsEn.put(sd.usrn, street.pack)
      case "CYM" => forwardData.streetDescriptorsCy.put(sd.usrn, street.pack)
      case _ => // ignored
    }
  }

  def sizeInfo: String = s"First pass obtained ${forwardData.sizeInfo}."
}
