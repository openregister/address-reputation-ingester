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
import ingest.Ingester.Blpu
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import services.exec.Continuer
import services.model.{StateModel, StatusLogger}
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer

import scala.annotation.tailrec
import scala.collection.mutable

object Ingester {

  import addressbase._

  val StreetTypeOfficialDesignatedName = '1'
  val StreetTypeNotYetKnown = 'A'

  case class Blpu(parentUprn: Option[Long], postcode: String, logicalStatus: Char, subdivision: Char, localCustodianCode: Int) {
    private def pu = optLongToString(parentUprn)

    private def lcc = localCustodianCode.toString

    def pack: String = s"$pu|$postcode|$logicalStatus|$subdivision|$lcc"
  }

  object Blpu {
    def unpack(pack: String): Blpu = {
      require(pack != null)
      val fields = Divider.qsplit(pack, '|')
      val parentUprn = blankToOptLong(fields.head)
      val postcode = fields(1)
      val logicalStatus = blankToChar(fields(2))
      val subdivision = blankToChar(fields(3))
      val localCustodianCode = fields(4).toInt
      Blpu(parentUprn, postcode, logicalStatus, subdivision, localCustodianCode)
    }
  }

  case class Street(recordType: Char, streetDescription: String, localityName: String, townName: String) {
    def filteredDescription: String = if (recordType == StreetTypeOfficialDesignatedName) streetDescription else ""

    def pack: String = s"$recordType|$streetDescription|$localityName|$townName"
  }

  object Street {
    def unpack(pack: String): Street = {
      val fields = Divider.qsplit(pack, '|')
      val recordType = blankToChar(fields.head)
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


class Ingester(continuer: Continuer, settings: Algorithm, model: StateModel, statusLogger: StatusLogger, forwardData: ForwardData) {

  def ingestFromDir(rootDir: File, out: OutputWriter): Boolean = {
    val files = Ingester.listFiles(rootDir, ".zip").sorted
    val youngest = if (files.isEmpty) Ingester.theEpoch else new Date(files.map(_.lastModified).max)
    val target = out.existingTargetThatIsNewerThan(youngest)
    if (target.isEmpty) {
      statusLogger.info(s"Ingesting from $rootDir.")
      ingestFiles(files, out)
    } else if (model.forceChange) {
      statusLogger.info(s"Ingesting from $rootDir (forced update).")
      ingestFiles(files, out)
    } else {
      statusLogger.info(s"Ingest skipped; ${target.get} is up to date.")
      // Not strictly a failure, this inhibits an immediate automatic switch-over.
      true // failure
    }
  }

  private[ingest] def ingestFiles(files: Seq[File], out: OutputWriter): Boolean = {
    val dt = new DiagnosticTimer
    val fp = new FirstPass(out, continuer, settings, forwardData)
    out.begin()

    statusLogger.info(s"Starting first pass through ${files.size} files.")
    val fewerFiles = pass(files, out, fp)
    statusLogger.info(s"First pass complete after {}.", dt)

    val rfd = reduceDefaultedLCCs(forwardData)

    statusLogger.info(s"Starting second pass through ${fewerFiles.size} files.")
    val sp = new SecondPass(rfd, continuer, settings)
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
      } catch {
        case e: Exception =>
          statusLogger.warn(e.getMessage)
          statusLogger.tee.warn(e.getMessage, e)
      } finally {
        zip.close()
        statusLogger.info(s"Reading from ${zip.nFiles} CSV files in {} took {}.", file.getName, dt)
      }
      statusLogger.info(thisPass.sizeInfo)
    }
    passOn.toList
  }

  private[ingest] def reduceDefaultedLCCs(fd: ForwardData): ForwardData = {
    val dt = new DiagnosticTimer
    var count = 0
    val it = fd.blpu.entrySet.iterator
    while (it.hasNext) {
      val entry = it.next
      val uprn = entry.getKey
      val blpu = Blpu.unpack(entry.getValue)
      val reduced = reduce(uprn, blpu, fd)

      if (reduced.localCustodianCode != defaultLCC) {
        val replacement = blpu.copy(localCustodianCode = reduced.localCustodianCode)
        entry.setValue(replacement.pack)
        count += 1
      }
    }

    statusLogger.info(s"Default LCC reduction altered $count BLPUs and took {}.", dt)
    fd
  }

  @tailrec
  private def reduce(uprn: Long, blpu: Blpu, fd: ForwardData): Blpu = {
    if (blpu.localCustodianCode == defaultLCC && blpu.parentUprn.isDefined) {
      val parentUprn = blpu.parentUprn.get
      // n.b. using ChronicleMap, the containsKey test is vital because 'get' can return odd results (possible bug)
      if (parentUprn != uprn && fd.blpu.containsKey(parentUprn)) {
        val parent = Blpu.unpack(fd.blpu.get(parentUprn))
        reduce(blpu.parentUprn.get, parent, fd)
      } else {
        blpu
      }
    } else {
      blpu
    }
  }

  val defaultLCC = 7655
  val emptyBlpu = Blpu(None, "", ' ', ' ', 0)
}

class IngesterFactory {
  def ingester(continuer: Continuer, settings: Algorithm, model: StateModel, statusLogger: StatusLogger): Ingester =
    new Ingester(continuer, settings, model, statusLogger, ForwardData.chronicleInMemory(settings.prefer))
}

