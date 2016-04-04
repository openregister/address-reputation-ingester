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

import services.ingester.converter.extractor.{FirstPass, SecondPass}
import uk.co.hmrc.address.osgb.DbAddress

import scala.util.Try

object Extractor {

  case class Blpu(postcode: String, logicalStatus: Char)

  case class Street(recordType: Char, streetDescription: String = "", localityName: String = "", townName: String = "")

  private def listFiles(file: File): List[File] =
    if (!file.isDirectory) Nil
    else file.listFiles().filter(f => f.getName.toLowerCase.endsWith(".zip")).toList


  def extract(rootDir: File, out: (DbAddress) => Unit): Try[Int] = {
    val files = listFiles(rootDir).toVector
    val x = FirstPass.firstPass(files, out).flatMap {
      SecondPass.secondPass(files, _, out)
    }
    x.map(x => x.size)
  }
}

