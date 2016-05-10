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

package services.fetch

import java.io.{File, FileInputStream, InputStream}
import java.nio.file.Files
import java.util.zip.ZipInputStream

import uk.co.hmrc.logging.SimpleLogger
import Utils._

class ZipUnpacker(logger: SimpleLogger, unpackFolder: File) {

  def unzipList(zipFiles: List[File], destPath: String): Int = {
    zipFiles.map(z => unzip(z, destPath)).sum
  }

  def unzip(file: File, destPath: String): Int = {
    if (file.getName.toLowerCase.endsWith(".zip")) {
      unzip(new FileInputStream(file), destPath)
    } else 0
  }

  def unzip(zipFile: InputStream, destPath: String): Int = {
    val destDirectory = if (destPath.nonEmpty) new File(unpackFolder, destPath) else unpackFolder
    // remove all pre-existing files
    deleteDir(destDirectory)
    destDirectory.mkdirs()

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
