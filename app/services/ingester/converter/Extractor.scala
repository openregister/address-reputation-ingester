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
import services.ingester.exec.Task
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.logging.SimpleLogger

object Extractor {

  case class Blpu(postcode: String, logicalStatus: Char)

  case class Street(recordType: Char, streetDescription: String = "", localityName: String = "", townName: String = "") {
    def filteredDescription: String = if (recordType == '1') streetDescription else ""
  }

}


class Extractor(task: Task) {
  private def listFiles(file: File): List[File] =
    if (!file.isDirectory) Nil
    else file.listFiles().filter(f => f.getName.toLowerCase.endsWith(".zip")).toList


  def extract(rootDir: File, out: (DbAddress) => Unit, logger: SimpleLogger) {
    val dt = new DiagnosticTimer
    val files = listFiles(rootDir).toVector
    val fp = new FirstPass(out, task)

    for (file <- files) {
      if (task.isBusy) {
        LoadZip.zipReader(file, logger) {
          it =>
            fp.processFile(it, out)
            logger.info(fp.sizeInfo)
        }
      }
    }
    val fd = fp.firstPass
    logger.info(s"First pass complete at $dt")

    for (file <- files) {
      if (task.isBusy) {
        LoadZip.zipReader(file, logger) {
          it =>
            SecondPass.processFile(it, fd, out)
        }
      }
    }
    logger.info(s"Finished at $dt")
  }
}

class ExtractorFactory {
  def extractor(task: Task): Extractor = new Extractor(task)
}

