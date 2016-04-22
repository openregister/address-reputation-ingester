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

import services.ingester.converter._
import services.ingester.exec.Worker
import services.ingester.writers.OutputWriter

class SecondPass(fd: ForwardData, worker: Worker) extends Pass {

  def processFile(csvIterator: Iterator[Array[String]], out: OutputWriter) {
    for (csvLine <- csvIterator
         if worker.isBusy) {
      if (csvLine(OSCsv.RecordIdentifier_idx) == OSLpi.RecordId) {
        processLPI(csvLine, out)
      }
    }
  }

  private def processLPI(csvLine: Array[String], out: OutputWriter): Unit = {
    val lpi = OSLpi(csvLine)
    val blpu = fd.blpu.get(lpi.uprn)

    blpu match {
      case Some(b) if b.logicalStatus == lpi.logicalStatus =>
        out.output(ExportDbAddress.exportLPI(lpi, b, fd.streets))
        // TODO: this results in just accepting the first lpi record processed - really we should
        // be taking a decision in the firstPass which of the lpi records is the most suitable
        fd.blpu.remove(lpi.uprn)

      case _ =>
    }
  }

  def sizeInfo: String = ""
}

