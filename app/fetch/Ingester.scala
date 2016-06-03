/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package fetch

import java.io.File

import addressbase._
import db.OutputDBWriterFactory
import services.exec.Continuer
import services.model.StatusLogger
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

import scala.collection.Iterator

object Ingester {

}


class Ingester(logger: StatusLogger, continuer: Continuer, writerFactory: OutputDBWriterFactory, settings: WriterSettings, productName: String) {

  val blpuWriter = writerFactory.writer(productName + "_blpu", List("uprn"), logger, settings)
  val dpaWriter = writerFactory.writer(productName + "_dpa", List("uprn", "postcode"), logger, settings)
  val lpiWriter = writerFactory.writer(productName + "_lpi", List("uprn"), logger, settings)
  val sdWriter = writerFactory.writer(productName + "_streetdesc", List("usrn"), logger, settings)
  val streetWriter = writerFactory.writer(productName + "_street", List("usrn"), logger, settings)

  var blpuCount = 0
  var dpaCount = 0
  var lpiCount = 0
  var sdCount = 0
  var streetCount = 0

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
      logger.info(s"Reading from ${zip.nFiles} CSV files in ${file.getName} took {}." +
        s" BLPUs: $blpuCount, DPAs: $dpaCount LPIs: $lpiCount, Streets: $streetCount, Descr: $sdCount.", dt)
    }

    if (!continuer.isBusy) {
      abort()
    }
  }

  def begin() {
    blpuWriter.begin()
    dpaWriter.begin()
    lpiWriter.begin()
    sdWriter.begin()
    streetWriter.begin()

    blpuCount = 0
    dpaCount = 0
    lpiCount = 0
    sdCount = 0
    streetCount = 0
  }

  private def abort() {
    blpuWriter.abort()
    dpaWriter.abort()
    lpiWriter.abort()
    sdWriter.abort()
    streetWriter.abort()
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
          street(csvLine)

        case OSStreetDescriptor.RecordId =>
          streetDescriptor(csvLine)

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

  private def street(csvLine: Array[String]) {
    val street = OSStreet(csvLine)
    streetWriter.output(street.normalise)
    streetCount += 1
  }

  private def streetDescriptor(csvLine: Array[String]) {
    val sd = OSStreetDescriptor(csvLine)
    sdWriter.output(sd.normalise)
    sdCount += 1
  }

  private def lpi(csvLine: Array[String]) {
    val lpi = OSLpi(csvLine)
    lpiWriter.output(lpi.normalise)
    lpiCount += 1
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
  def ingester(logger: StatusLogger, continuer: Continuer, writerFactory: OutputDBWriterFactory, settings: WriterSettings, productName: String): Ingester =
    new Ingester(logger, continuer, writerFactory, settings, productName)
}

