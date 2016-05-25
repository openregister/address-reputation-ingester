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

package ingest

import java.io.File

import ingest.writers.OutputDBWriter
import services.exec.Continuer
import services.model.StatusLogger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

import scala.collection.{Iterator, mutable}

object Ingester {

}


class Ingester(logger: StatusLogger, continuer: Continuer) {

  private def pass(files: Seq[File], out: OutputDBWriter): Seq[File] = {
    val passOn = new mutable.ListBuffer[File]()
    for (file <- files
         if continuer.isBusy) {
      val dt = new DiagnosticTimer
      val zip = LoadZip.zipReader(file, (name) => {
        name.toLowerCase.endsWith(".csv")
      })
      try {
        var neededLater = false
        while (zip.hasNext && continuer.isBusy) {
          val next = zip.next
          val name = next.zipEntry.getName
          logger.info(s"Reading zip entry $name...")
          val r = processFile(next, out)
          neededLater ||= r
        }
        if (neededLater) {
          passOn += file
        }
      } finally {
        zip.close()
        logger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}.", file.getName, dt)
      }
    }
    passOn.toList
  }

  // scalastyle:off
  private def processFile(csvIterator: Iterator[Array[String]], out: OutputDBWriter): Boolean = {
    var needSecondPass = false

    for (csvLine <- csvIterator
         if continuer.isBusy) {

      csvLine(OSCsv.RecordIdentifier_idx) match {
        case OSHeader.RecordId =>
          OSCsv.setCsvFormatFor(csvLine(OSHeader.Version_Idx))

        case OSBlpu.RecordId =>
          processBlpu(csvLine)

        case OSLpi.RecordId =>
          processLPI(csvLine)

        case OSDpa.RecordId =>
          processDpa(csvLine)
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

  private def processBlpu(csvLine: Array[String]): Unit = {
    //    val blpu = OSBlpu(csvLine)
    //    forwardData.blpu.put(blpu.uprn, Blpu(blpu.postcode, blpu.logicalStatus).pack)
  }

  private def processDpa(csvLine: Array[String]): Unit = {
    //    val osDpa = OSDpa(csvLine)
    //    forwardData.dpa.add(osDpa.uprn)
  }

  private def processStreet(street: OSStreet): Unit = {
    //    if (forwardData.streets.containsKey(street.usrn)) {
    //      val existingStreetStr = forwardData.streets.get(street.usrn)
    //      val existingStreet = Street.unpack(existingStreetStr)
    //      forwardData.streets.put(street.usrn, Street(street.recordType, existingStreet.streetDescription, existingStreet.localityName, existingStreet.townName).pack)
    //    } else {
    //      forwardData.streets.put(street.usrn, Street(street.recordType, "", "", "").pack)
    //    }
  }

  private def processStreetDescriptor(sd: OSStreetDescriptor) {
    //    if (forwardData.streets.containsKey(sd.usrn)) {
    //      val existingStreetStr = forwardData.streets.get(sd.usrn)
    //      val existingStreet = Street.unpack(existingStreetStr)
    //      forwardData.streets.put(sd.usrn, Street(existingStreet.recordType, sd.description, sd.locality, sd.town).pack)
    //    } else {
    //      forwardData.streets.put(sd.usrn, Street('A', sd.description, sd.locality, sd.town).pack)
    //    }
  }

  private def processLPI(csvLine: Array[String]) {
    //    val lpi = OSLpi(csvLine)
    //
    //    if (!fd.dpa.contains(lpi.uprn)) {
    //      if (fd.blpu.containsKey(lpi.uprn)) {
    //        val packedBlpu = fd.blpu.get(lpi.uprn)
    //        val blpu = Blpu.unpack(packedBlpu)
    //
    //        if (blpu.logicalStatus == lpi.logicalStatus) {
    //          out.output(ExportDbAddress.exportLPI(lpi, blpu.postcode, fd.streets))
    //          lpiCount += 1
    //          fd.blpu.remove(lpi.uprn) // need to decide which lpi to use in the firstPass using logic - not first in gets in
    //        }
    //      }
    //    }
  }

  private def processDPA(csvLine: Array[String]): Unit = {
    //    val dpa = OSDpa(csvLine)
    //    out.output(ExportDbAddress.exportDPA(dpa))
    //    dpaCount += 1
  }
}


case class WriterSettings(bulkSize: Int, loopDelay: Int)

object WriterSettings {
  val default = WriterSettings(1, 0)
}


class IngesterFactory {
  def ingester(continuer: Continuer, statusLogger: StatusLogger): Ingester =
    new Ingester(statusLogger, continuer)
}

