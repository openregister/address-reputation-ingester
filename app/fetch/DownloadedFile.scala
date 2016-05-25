/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package fetch

import java.io.File
import java.nio.file.{Files, Path}


case class DownloadedFile(file: File) {
  def this(fpath: String) = this(new File(fpath))

  val isZipFile: Boolean = file.getName.toLowerCase.endsWith(".zip")

  val doneFile = new File(file.getParentFile, file.getName + ".done")

  def product: String = file.getParentFile.getParentFile.getParentFile.getName

  def epoch: Int = file.getParentFile.getParentFile.getName.toInt

  def variant: String = file.getParentFile.getName

  def getName: String = file.getName

  def length: Long = file.length()

  def toPath: Path = file.toPath

  def exists: Boolean = file.exists

  // Indicates a partial / failed download.
  def isIncomplete: Boolean = !doneFile.exists || file.lastModified > doneFile.lastModified

  def delete() {
    file.delete()
    doneFile.delete()
  }

  def touchDoneFile() {
    if (!doneFile.exists)
      Files.createFile(doneFile.toPath)
  }
}

object DownloadedFile {
  def apply(dir: File, name: String): DownloadedFile = new DownloadedFile(new File(dir, name))
}


case class DownloadItem(file: DownloadedFile, fresh: Boolean)

object DownloadItem {
  def fresh(f: DownloadedFile): DownloadItem = new DownloadItem(f, true)

  def stale(f: DownloadedFile): DownloadItem = new DownloadItem(f, false)
}

