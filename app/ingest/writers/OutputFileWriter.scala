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
 */

package ingest.writers

import java.io.{OutputStreamWriter, _}
import java.util.Date
import java.util.zip.GZIPOutputStream

import controllers.ControllerConfig
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.osgb.DbAddress

import scala.concurrent.ExecutionContext


class OutputFileWriter(var model: StateModel, statusLogger: StatusLogger, fieldSeparator: String = "\t") extends OutputWriter {

  val fileRoot = model.collectionName.toString
  val kind = if (fieldSeparator == "\t") "tsv" else "txt"
  val outputFile = new File(ControllerConfig.outputFolder, s"$fileRoot.$kind.gz")

  private val bufSize = 32 * 1024
  private var outCSV: PrintWriter = _

  private var count = 0

  def existingTargetThatIsNewerThan(date: Date): Option[String] =
    if (outputFile.exists() && outputFile.lastModified() >= date.getTime)
      Some(outputFile.getPath)
    else
      None

  def begin() {
    ControllerConfig.outputFolder.mkdirs()
    val outfile = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), bufSize))
    outCSV = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outfile), bufSize))
  }

  def output(a: DbAddress) {
    // scalastyle:off
    outCSV.println(string(a))
    count += 1
  }

  private def string(a: DbAddress) = {
    // allow the fields to be changed without needing to rewrite this often
    val fields = a.productIterator.toList
    val asStrings: List[String] = fields.map {
      case list: List[_] => list.mkString(":")
      case Some(v) => v.toString
      case None => ""
      case x => x.toString
    }
    asStrings.mkString(fieldSeparator)
  }

  def end(completed: Boolean): StateModel = {
    if (outCSV.checkError()) {
      statusLogger.warn(s"Failed whilst writing to $outputFile")
      model = model.copy(hasFailed = true)
    }
    outCSV.close()
    println(s"*** document count = $count")
    model
  }
}


class OutputFileWriterFactory extends OutputWriterFactory {
  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings, ec: ExecutionContext) =
    new OutputFileWriter(model, statusLogger)
}
