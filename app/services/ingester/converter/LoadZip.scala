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

import org.apache.commons.compress.archivers.zip.ZipFile
import uk.co.hmrc.address.services.CsvParser

import scala.util.Try


case class EmptyFileException(msg: String) extends Exception(msg)


object LoadZip {

  def file2ZipFile(f: File): ZipFile = new ZipFile(f)

  def zipReader[T](zipFile: ZipFile)(consumer: (Iterator[Array[String]]) => T): Try[T] = {
    Try {
      val enumeration = zipFile.getEntries
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
        }
      }
    }
  }
}
