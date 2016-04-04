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

import org.apache.commons.compress.archivers.zip.ZipFile
import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import uk.co.hmrc.address.osgb.DbAddress

import scala.collection.immutable.HashMap
import scala.util.Try

object SecondPass {

  def secondPass(zipFiles: Vector[ZipFile], fd: ForwardData, out: (DbAddress) => Unit): Try[HashMap[Long, Blpu]] = Try {
    zipFiles.foldLeft(fd.blpu) {
      case (remainingBLPU0, file) =>
        LoadZip.zipReader(file) {
          csvIterator =>
            processLine(csvIterator, remainingBLPU0, fd.streets, out)
        }.get
    }
  }


  def exportLPI(lpi: OSLpi, blpu: Blpu, streetList: HashMap[Long, Street])(out: (DbAddress) => Unit): Unit = {
    val street = streetList.getOrElse(lpi.usrn, Street('X', "<SUnknown>", "<SUnknown>", "<TUnknown>"))

    def numRange(sNum: String, sSuf: String, eNum: String, eSuf: String): String = {
      val start = (sNum + sSuf).trim
      val end = (eNum + eSuf).trim
      (start, end) match {
        case ("", "") => ""
        case (s, "") => s
        case ("", e) => e
        case (s, e) => s + "-" + e
      }
    }

    val line1 = (lpi.saoText + " " +
      numRange(lpi.saoStartNumber, lpi.saoStartSuffix, lpi.saoEndNumber, lpi.saoEndSuffix) + " " +
      lpi.paoText
      ).trim


    def streetDes(s: Street): String = if (s.recordType == '1') s.streetDescription else ""

    val line2 = (numRange(lpi.paoStartNumber, lpi.paoStartSuffix, lpi.paoEndNumber, lpi.paoEndSuffix) + " " +
      streetDes(street)).trim

    val line3 = street.localityName

    val line = DbAddress(
      "GB" + lpi.uprn.toString,
      OSCleanup.removeUninterestingStreets(line1),
      OSCleanup.removeUninterestingStreets(line2),
      OSCleanup.removeUninterestingStreets(line3),
      street.townName,
      blpu.postcode)

    out(line)
  }


  private[extractor] def processLine(csvIterator: Iterator[Array[String]], remainingBLPU0: HashMap[Long, Blpu],
                                     streetList: HashMap[Long, Street], out: (DbAddress) => Unit): HashMap[Long, Blpu] = {
    csvIterator.foldLeft(remainingBLPU0) {
      (blpuList, csvLine) =>
        if (csvLine(OSCsv.RecordIdentifier_idx) == OSLpi.RecordId) {
          val lpi = OSLpi(csvLine)
          val blpu = blpuList.get(lpi.uprn)

          blpu match {
            case Some(b) if b.logicalStatus == lpi.logicalStatus =>
              exportLPI(lpi, b, streetList)(out)
              blpuList - lpi.uprn
            case _ => blpuList
          }

        } else blpuList
    }
  }

}
