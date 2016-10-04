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

import java.io._
import java.util.zip.{ZipEntry, ZipFile}

import uk.gov.hmrc.address.services.CsvParser


case class EmptyFileException(msg: String) extends Exception(msg)


object LoadZip {
  def zipReader(file: File, accept: (String) => Boolean = (_) => true): ZipWrapper = new ZipWrapper(file, accept)
}


class ZipWrapper(zipFile: ZipFile, accept: (String) => Boolean) extends Iterator[ZippedCsvIterator] with Closeable {

  def this(file: File, accept: (String) => Boolean) = this(new ZipFile(file), accept)

  private var open = true
  private var files = 0
  private val enumeration = zipFile.entries
  private var nextCache: Option[ZippedCsvIterator] = None

  if (!enumeration.hasMoreElements) {
    throw EmptyFileException("Empty file")
  }

  private def lookAhead() {
    nextCache = None
    while (enumeration.hasMoreElements && nextCache.isEmpty) {
      val zipEntry = enumeration.nextElement()
      if (accept(zipEntry.getName)) {
        nextCache = Some(new ZippedCsvIterator(zipFile.getInputStream(zipEntry), zipEntry, this))
      }
    }
  }

  lookAhead()

  override def hasNext: Boolean = open && nextCache.isDefined

  override def next: ZippedCsvIterator = {
    val thisNext = nextCache.get
    lookAhead()
    files += 1
    thisNext
  }

  /** Closes the entire ZIP archive. Should be done exactly once after reading all contents. */
  override def close() {
    if (open) {
      open = false
      zipFile.close()
    }
  }

  override def toString: String = zipFile.toString

  def nFiles = files
}


class ZippedCsvIterator(is: InputStream, val zipEntry: ZipEntry, container: Closeable) extends Iterator[Array[String]] {
  private var open = true
  private val data = new InputStreamReader(is)
  private val it = CsvParser.split(data)

  override def hasNext: Boolean = open && it.hasNext

  override def next: Array[String] = {
    try {
      it.next
    } catch {
      case e: Exception =>
        e.printStackTrace()
        close()
        throw e
    }
  }

  /** Closes the entire ZIP archive. Should be done exactly once after reading all contents. */
  def close() {
    if (open) {
      open = false
      container.close()
    }
  }

  override def toString: String = zipEntry.toString
}
