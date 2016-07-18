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
import java.util.Date

import config.Divider
import services.exec.Continuer
import services.model.{StateModel, StatusLogger}
import ingest.writers.OutputWriter
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

import scala.collection.mutable

object Ingester {

  case class Blpu(postcode: String, logicalStatus: Char, subCountry: Char) {
    def pack: String = s"$postcode|$logicalStatus|$subCountry"
  }

  object Blpu {
    def unpack(pack: String): Blpu = {
      val fields = Divider.qsplit(pack, '|')
      val logicalStatus = if (fields(1).length > 0) fields(1).charAt(0) else '\u0000'
      val subCountry = if (fields(2).length > 0) fields(2).charAt(0) else '\u0000'
      Blpu(fields.head, logicalStatus, subCountry)
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

  val theEpoch = new Date(0)
}


class Ingester(continuer: Continuer, model: StateModel, statusLogger: StatusLogger, forwardData: ForwardData) {

  def ingest(rootDir: File, out: OutputWriter): Boolean = {
    val files = Ingester.listFiles(rootDir, ".zip").sorted
    val youngest = if (files.isEmpty) Ingester.theEpoch else new Date(files.map(_.lastModified).max)
    val target = out.existingTargetThatIsNewerThan(youngest)
    if (target.isEmpty) {
      statusLogger.info(s"Ingesting from $rootDir.")
      ingest(files, out)
    } else if (model.forceChange) {
      statusLogger.info(s"Ingesting from $rootDir (forced update).")
      ingest(files, out)
    } else {
      statusLogger.info(s"Ingest skipped; ${target.get} is up to date.")
      // Not strictly a failure, this inhibits an immediate automatic switch-over.
      true // failure
    }
  }

  private[ingest] def ingest(files: Seq[File], out: OutputWriter): Boolean = {
    val dt = new DiagnosticTimer
    val fp = new FirstPass(out, continuer, forwardData)
    out.begin()

    statusLogger.info(s"Starting first pass through ${files.size} files.")
    val fewerFiles = pass(files, out, fp)
    val fd = fp.firstPass
    statusLogger.info(s"First pass complete after {}.", dt)

    statusLogger.info(s"Starting second pass through ${fewerFiles.size} files.")
    val sp = new SecondPass(fd, continuer)
    pass(fewerFiles, out, sp)
    statusLogger.info(s"Ingester finished after {}.", dt)

    forwardData.close() // release shared memory etc
    false // not a failure
  }


  private def pass(files: Seq[File], out: OutputWriter, thisPass: Pass): Seq[File] = {
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
          statusLogger.info(s"Reading zip entry $name...")
          val r = thisPass.processFile(next, out)
          neededLater ||= r
        }
        if (neededLater) {
          passOn += file
        }
      } finally {
        zip.close()
        statusLogger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}.", file.getName, dt)
      }
      statusLogger.info(thisPass.sizeInfo)
    }
    passOn.toList
  }
}

class IngesterFactory {
  def ingester(continuer: Continuer, model: StateModel, statusLogger: StatusLogger): Ingester =
    new Ingester(continuer, model, statusLogger, ForwardData.chronicleInMemory())
}

