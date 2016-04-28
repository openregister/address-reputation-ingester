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

package services.ingester.converter

import java.io.File

import services.ingester.converter.extractor.{FirstPass, Pass, SecondPass}
import services.ingester.exec.Continuer
import services.ingester.writers.OutputWriter
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.SimpleLogger
import config.Divider

object Extractor {

  case class Blpu(postcode: String, logicalStatus: Char) {
    def pack: String = s"$postcode|$logicalStatus"
  }

  object Blpu {
    def unpack(pack: String): Blpu = {
      val fields = Divider.qsplit(pack, '|')
      val logicalStatus = if (fields(1).length > 0) fields(1).charAt(0) else '\u0000'
      Blpu(fields.head, logicalStatus)
    }
  }

  case class Street(recordType: Char, streetDescription: String, localityName: String, townName: String) {
    def filteredDescription: String = if (recordType == '1') streetDescription else ""

    def pack: String = s"$recordType|$streetDescription|$localityName|$townName"
  }

  object Street {
    def unpack(pack: String): Street = {
      val fields = Divider.qsplit(pack, '|')
      val recordType = if (fields.head.length > 0) fields.head.charAt(0) else '\u0000'
      Street(recordType, fields(1), fields(2), fields(3))
    }
  }

}


class Extractor(continuer: Continuer, logger: SimpleLogger) {
  private def listFiles(file: File): List[File] =
    if (!file.isDirectory) Nil
    else file.listFiles().filter(f => f.getName.toLowerCase.endsWith(".zip")).toList


  def extract(rootDir: File, out: OutputWriter) {
    logger.info(s"Ingesting from $rootDir")
    extract(listFiles(rootDir), out)
  }

  def extract(files: Seq[File], out: OutputWriter) {
    val dt = new DiagnosticTimer
    val fp = new FirstPass(out, continuer)

    logger.info(s"Starting first pass through ${files.size} files")
    pass(files, out, fp)
    val fd = fp.firstPass
    logger.info(s"First pass complete after {}", dt)

    logger.info(s"Starting second pass through ${files.size} files")
    val sp = new SecondPass(fd, continuer)
    pass(files, out, sp)
    logger.info(s"Finished after {}", dt)
  }

  private def pass(files: Seq[File], out: OutputWriter, thisPass: Pass) {
    for (file <- files
         if continuer.isBusy) {
      val dt = new DiagnosticTimer
      val zip = LoadZip.zipReader(file, (name) => {
        name.toLowerCase.endsWith(".csv")
      })
      try {
        while (zip.hasNext && continuer.isBusy) {
          val next = zip.next
          val name = next.zipEntry.getName
          logger.info(s"Reading zip entry $name...")
          thisPass.processFile(next, out)
        }
      } finally {
        zip.close()
        logger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}", file.getName, dt)
      }
      logger.info(thisPass.sizeInfo)
    }
  }
}

class ExtractorFactory {
  def extractor(continuer: Continuer, logger: SimpleLogger): Extractor = new Extractor(continuer, logger)
}

