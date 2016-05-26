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

import addressbase._
import ingest.writers.OutputDBWriterFactory
import services.exec.Continuer
import services.model.StatusLogger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

import scala.collection.Iterator

object Ingester {

}


class Ingester(logger: StatusLogger, continuer: Continuer, writerFactory: OutputDBWriterFactory, settings: WriterSettings) {

  val blpuWriter = writerFactory.writer("osgb_blpu", List("uprn"), logger, settings)
  val dpaWriter = writerFactory.writer("osgb_dpa", List("uprn", "postcode"), logger, settings)

  blpuWriter.begin()
  dpaWriter.begin()

  var blpuCount = 0
  var dpaCount = 0

  def ingestZip(file: File) {
    val dt = new DiagnosticTimer
    val zip = LoadZip.zipReader(file, (name) => {
      name.toLowerCase.endsWith(".csv")
    })
    try {
      while (zip.hasNext && continuer.isBusy) {
        val next = zip.next
        val name = next.zipEntry.getName
        val n = processFile(next)
        logger.info(s"Read $n lines from zip entry $name.")
      }
    } finally {
      zip.close()
      logger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}. BLPUs: $blpuCount, DPAs: $dpaCount.", file.getName, dt)
    }
  }

  // scalastyle:off
  private def processFile(csvIterator: Iterator[Array[String]]) = {
    var n = 0
    for (csvLine <- csvIterator
         if continuer.isBusy) {

      csvLine(OSCsv.RecordIdentifier_idx) match {
        case OSHeader.RecordId =>
          OSCsv.setCsvFormatFor(csvLine(OSHeader.Version_Idx))

        case OSBlpu.RecordId =>
          blpu(csvLine)

        case OSLpi.RecordId =>
          lpi(csvLine)

        case OSDpa.RecordId =>
          dpa(csvLine)

        case OSStreet.RecordId =>
          street(OSStreet(csvLine))

        case OSStreetDescriptor.RecordId if OSStreetDescriptor.isEnglish(csvLine) =>
          streetDescriptor(OSStreetDescriptor(csvLine))

        case _ =>
      }
      n += 1
    }
    n
  }

  private def blpu(csvLine: Array[String]) {
    val blpu = OSBlpu(csvLine)
    blpuWriter.output(blpu.normalise)
    blpuCount += 1
  }

  private def street(street: OSStreet) {
    //    if (forwardData.streets.containsKey(street.usrn)) {
    //      val existingStreetStr = forwardData.streets.get(street.usrn)
    //      val existingStreet = Street.unpack(existingStreetStr)
    //      forwardData.streets.put(street.usrn, Street(street.recordType, existingStreet.streetDescription, existingStreet.localityName, existingStreet.townName).pack)
    //    } else {
    //      forwardData.streets.put(street.usrn, Street(street.recordType, "", "", "").pack)
    //    }
  }

  private def streetDescriptor(sd: OSStreetDescriptor) {
    //    if (forwardData.streets.containsKey(sd.usrn)) {
    //      val existingStreetStr = forwardData.streets.get(sd.usrn)
    //      val existingStreet = Street.unpack(existingStreetStr)
    //      forwardData.streets.put(sd.usrn, Street(existingStreet.recordType, sd.description, sd.locality, sd.town).pack)
    //    } else {
    //      forwardData.streets.put(sd.usrn, Street('A', sd.description, sd.locality, sd.town).pack)
    //    }
  }

  private def lpi(csvLine: Array[String]) {
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

  private def dpa(csvLine: Array[String]) {
    val dpa = OSDpa(csvLine)
    dpaWriter.output(dpa.normalise)
    dpaCount += 1
  }
}


case class WriterSettings(bulkSize: Int, loopDelay: Int)

object WriterSettings {
  val default = WriterSettings(1, 0)
}


class IngesterFactory {
  def ingester(logger: StatusLogger, continuer: Continuer, writerFactory: OutputDBWriterFactory, settings: WriterSettings): Ingester =
    new Ingester(logger, continuer, writerFactory, settings)
}

