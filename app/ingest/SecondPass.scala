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
import services.exec.Continuer
import ingest.Ingester.Blpu
import ingest.writers.OutputWriter

class SecondPass(fd: ForwardData, continuer: Continuer) extends Pass {

  private var dpaCount = 0
  private var lpiCount = 0


  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter): Boolean = {
    for (csvLine <- csvIterator
         if continuer.isBusy) {

      csvLine(OSCsv.RecordIdentifier_idx) match {
        case OSHeader.RecordId =>
          OSCsv.setCsvFormatFor(csvLine(OSHeader.Version_Idx))

        case OSLpi.RecordId => processLPI(csvLine, out)

        case OSDpa.RecordId => processDPA(csvLine, out)

        case _ =>
      }
    }
    false
  }

  private def processLPI(csvLine: Array[String], out: OutputWriter): Unit = {
    val lpi = OSLpi(csvLine)

    if (!fd.dpa.contains(lpi.uprn)) {
      if (fd.blpu.containsKey(lpi.uprn)) {
        val packedBlpu = fd.blpu.get(lpi.uprn)
        val blpu = Blpu.unpack(packedBlpu)

        if (blpu.logicalStatus == lpi.logicalStatus) {
          out.output(ExportDbAddress.exportLPI(lpi, blpu.postcode, fd.streets))
          lpiCount += 1
          fd.blpu.remove(lpi.uprn) // need to decide which lpi to use in the firstPass using logic - not first in gets in
        }
      }
    }
  }

  private def processDPA(csvLine: Array[String], out: OutputWriter): Unit = {
    val dpa = OSDpa(csvLine)
    out.output(ExportDbAddress.exportDPA(dpa))
    dpaCount += 1
  }

  def sizeInfo: String = s"Second pass processed $dpaCount DPAs, $lpiCount LPIs."
}

