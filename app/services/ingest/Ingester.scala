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

package services.ingest

import java.io.File

import config.Divider
import services.exec.Continuer
import services.model.{StateModel, StatusLogger}
import services.writers.OutputWriter
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

object Ingester {

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

  private[ingest] def listFiles(file: File, extn: String): List[File] =
    if (!file.isDirectory) Nil
    else {
      val (dirs, files) = file.listFiles().toList.partition(_.isDirectory)
      val zips = files.filter(f => f.getName.toLowerCase.endsWith(extn))
      val deeper = dirs.flatMap(listFiles(_, extn))
      zips.sorted ++ deeper
    }
}


class Ingester(continuer: Continuer, model: StateModel, statusLogger: StatusLogger, forwardData: ForwardData = ForwardData.chronicleInMemory()) {

  def ingest(rootDir: File, out: OutputWriter): StateModel = {
    statusLogger.info(s"Ingesting from $rootDir.")
    ingest(Ingester.listFiles(rootDir, ".zip"), out)
  }

  private[ingest] def ingest(files: Seq[File], out: OutputWriter): StateModel = {
    val dt = new DiagnosticTimer
    val fp = new FirstPass(out, continuer, forwardData)

    statusLogger.info(s"Starting first pass through ${files.size} files.")
    pass(files, out, fp)
    val fd = fp.firstPass
    statusLogger.info(s"First pass complete after {}.", dt)

    statusLogger.info(s"Starting second pass through ${files.size} files.")
    val sp = new SecondPass(fd, continuer)
    pass(files, out, sp)
    statusLogger.info(s"Ingester finished after {}.", dt)

    model // unchanged
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
          statusLogger.info(s"Reading zip entry $name...")
          thisPass.processFile(next, out)
        }
      } finally {
        zip.close()
        statusLogger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}.", file.getName, dt)
      }
      statusLogger.info(thisPass.sizeInfo)
    }
  }
}

class IngesterFactory {
  def ingester(continuer: Continuer, model: StateModel, statusLogger: StatusLogger): Ingester =
    new Ingester(continuer, model, statusLogger)
}

