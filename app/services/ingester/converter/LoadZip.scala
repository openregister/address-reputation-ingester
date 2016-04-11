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

import java.io._
import java.util.zip.ZipFile

import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.SimpleLogger


case class EmptyFileException(msg: String) extends Exception(msg)


object LoadZip {

  def zipReader[T](file: File, logger: SimpleLogger)(consumer: (Iterator[Array[String]]) => T): T = {
    val dt = new DiagnosticTimer
    val t = innerZipReader(file)(consumer)
    logger.info(s"Reading from $file took $dt")
    t
  }

  def innerZipReader[T](file: File)(consumer: (Iterator[Array[String]]) => T): T = {
    val zipFile = new ZipFile(file)
    val enumeration = zipFile.entries
    if (!enumeration.hasMoreElements) {
      throw EmptyFileException("Empty file")

    } else {
      val zipEntry = zipFile.getInputStream(enumeration.nextElement())
      try {
        val data = new InputStreamReader(zipEntry)
        val it = CsvParser.split(data)
        consumer(it)
      } finally {
        zipEntry.close()
        zipFile.close()
      }
    }
  }
}
