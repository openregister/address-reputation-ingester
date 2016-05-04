/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package services.ingester.fetch

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.Files
import java.util.zip.ZipInputStream

import uk.co.hmrc.logging.SimpleLogger

class ZipUnpacker(logger: SimpleLogger) {

  /**
    * Extracts a zip file to a directory, which will be created if does not exist.
    */
  def unzip(zipFile: File, destDirectory: File): Int = {
    if (!destDirectory.exists()) {
      destDirectory.mkdirs()
    }
    unzip(new FileInputStream(zipFile), destDirectory)
  }

  /**
    * Extracts a zip file to a directory, which will be created if does not exist.
    */
  def unzip(zipFile: InputStream, destDirectory: File): Int = {
    var files = 0
    val zipIn = new ZipInputStream(zipFile)
    try {
      var entry = Option(zipIn.getNextEntry)
      while (entry.isDefined) {
        val file = entry.get
        val filePath = new File(destDirectory, file.getName)
        if (file.isDirectory) {
          logger.info("mkdir {}", file.getName)
          filePath.mkdir()
        } else {
          logger.info("copy {}", file.getName)
          Files.copy(zipIn, filePath.toPath)
          files += 1
        }
        zipIn.closeEntry()
        entry = Option(zipIn.getNextEntry)
      }
    } finally {
      zipIn.close()
    }
    files
  }
}
