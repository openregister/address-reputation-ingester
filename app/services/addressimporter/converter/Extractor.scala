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

package services.addressimporter.converter

import java.io.File

import org.apache.commons.compress.archivers.zip.ZipFile
import services.addressimporter.converter.Extractor.{Blpu, Street}
import services.addressimporter.converter.extractor.{FirstPass, SecondPass}

import scala.collection.immutable.{HashMap, HashSet}
import scala.util.Try

object Extractor {

  case class Blpu(postcode: String, logicalStatus: Char)

  case class Street(recordType: Char, streetDescription: String = "", localityName: String = "", townName: String = "")

  def listFiles(file: File): Option[Vector[ZipFile]] =
    if (!file.isDirectory) None
    else Some(file.listFiles().filter(f => f.getName.toLowerCase.endsWith(".zip")).map(LoadZip.file2ZipFile).toVector)


  def extract(rootDir: File, out: (CSVLine) => Unit): Option[Try[Int]] = {
    val x = listFiles(rootDir)
      .map { zipFiles =>
        FirstPass.firstPass(zipFiles, out).flatMap {
          SecondPass.secondPass(zipFiles, _, out)
        }
      }
    x.map(t => t.map(x => x.size))
  }
}


object ForwardData {
  def empty: ForwardData = ForwardData(HashMap.empty[Long, Blpu], HashSet.empty[Long], HashMap.empty[Long, Street], HashMap.empty[Long, Byte])
}

case class ForwardData(blpu: HashMap[Long, Blpu], dpa: HashSet[Long], streets: HashMap[Long, Street], lpiLogicStatus: HashMap[Long, Byte]) {
  def update(fd: ForwardData): ForwardData = {
    val totalDpa = dpa ++ fd.dpa
    val totalBlpu = blpu ++: fd.blpu
    val remainingBlpu = totalDpa.foldLeft(totalBlpu) { (b, d) => b - d }
    val remainingDpa = totalDpa -- fd.blpu.keySet // just try and keep the memory down, fill not delete everything

    ForwardData(remainingBlpu, remainingDpa, fd.streets, fd.lpiLogicStatus)
  }
}


