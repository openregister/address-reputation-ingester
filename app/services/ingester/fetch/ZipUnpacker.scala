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

import java.io.{File, FileInputStream}
import java.nio.file.Files
import java.util.zip.ZipInputStream

import uk.co.hmrc.logging.{SimpleLogger, Stdout}

class ZipUnpacker(logger: SimpleLogger) {

  /**
    * Extracts a zip file specified by the zipFilePath to a directory specified by
    * destDirectory (will be created if does not exists)
    */
  def unzip(zipFile: File, destDirectory: File) {
    if (!destDirectory.exists()) {
      destDirectory.mkdirs()
    }
    val zipIn = new ZipInputStream(new FileInputStream(zipFile))
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
        }
        zipIn.closeEntry()
        entry = Option(zipIn.getNextEntry)
      }
    } finally {
      zipIn.close()
    }
  }
}
