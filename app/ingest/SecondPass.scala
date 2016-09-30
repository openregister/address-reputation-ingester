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

import addressbase.{OSCsv, OSDpa, OSHeader, OSLpi}
import ingest.Ingester.{Blpu, Street, StreetDescriptor}
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import services.exec.Continuer

class SecondPass(fd: ForwardData, continuer: Continuer, settings: Algorithm) extends Pass {

  private var dpaCount = 0
  private var lpiCount = 0


  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter): Boolean = {
    for (csvLine <- csvIterator
         if continuer.isBusy) {

      csvLine(OSCsv.RecordIdentifier_idx) match {
        case OSHeader.RecordId =>
          OSCsv.setCsvFormatFor(csvLine(OSHeader.Version_Idx))

        case OSLpi.RecordId if settings.includeLpi =>
          processLPI(csvLine, out)

        case OSDpa.RecordId if settings.includeDpa =>
          processDPA(csvLine, out)

        case _ =>
      }
    }
    false
  }

  private def processLPI(csvLine: Array[String], out: OutputWriter): Unit = {
    val lpi = OSLpi(csvLine).normalise

    if (settings.prefer == "LPI" || !fd.uprns.contains(lpi.uprn)) {
      val packedBlpu = Option(fd.blpu.get(lpi.uprn))
      if (packedBlpu.isDefined) {
        val blpu = Blpu.unpack(packedBlpu.get)

        if (blpu.logicalState == lpi.logicalState) {
          val optStreet = Option(fd.streets.get(lpi.usrn)).map(s => Street.unpack(s))
          val street = optStreet.getOrElse(defaultStreet)
          val optStreetDescEn = Option(fd.streetDescriptorsEn.get(lpi.usrn)).map(s => StreetDescriptor.unpack(s))
          val streetDescriptorEn = optStreetDescEn.getOrElse(defaultStreetDescriptor)
          // TODO Welsh not yet implemented

          val a = ExportDbAddress.exportLPI(lpi, blpu, street, streetDescriptorEn, settings)
          out.output(a)
          lpiCount += 1
          fd.blpu.remove(lpi.uprn) // need to decide which lpi to use in the firstPass using logic - first in gets in
        }
      }
    }
  }

  val defaultStreet = Street('X', "")
  val defaultStreetDescriptor = StreetDescriptor("<SUnknown>", "<SUnknown>", "<TUnknown>")

  private def processDPA(csvLine: Array[String], out: OutputWriter): Unit = {
    //TODO this only ingests English, ignoring any Welsh that is provided
    val dpa = OSDpa(csvLine).normalise

    if (settings.prefer == "DPA" || !fd.uprns.contains(dpa.uprn)) {
      val packedBlpu = Option(fd.blpu.get(dpa.uprn))

      val blpu = Blpu.unpack(packedBlpu.get)

      val a = ExportDbAddress.exportDPA(dpa, blpu, "en")
      // TODO Welsh not yet implemented
      out.output(a)
      dpaCount += 1
    }
  }

  def sizeInfo: String = s"Second pass processed $dpaCount DPAs, $lpiCount LPIs."
}
