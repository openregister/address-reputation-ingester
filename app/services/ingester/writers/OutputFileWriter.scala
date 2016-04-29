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

package services.ingester.writers

import java.io.{OutputStreamWriter, _}
import java.util.zip.GZIPOutputStream

import config.ConfigHelper._
import play.api.Play._
import services.ingester.model.ABPModel
import uk.co.hmrc.address.osgb.DbAddress

object OutputFileWriterHelper {
  val home = System.getenv("HOME")
  val outputFolder = new File(mustGetConfigString(current.mode, current.configuration, "app.files.outputFolder").replace("$HOME", home))
  outputFolder.mkdirs()
}


class OutputFileWriter(outputFile: File) extends OutputWriter {

  private val bufSize = 32 * 1024
  private val outfile = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), bufSize))
  private val outCSV = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outfile), bufSize))

  private var count = 0

  override def output(a: DbAddress) {
    // scalastyle:off
    outCSV.println(a.toString)
    count += 1
  }

  override def close(): Unit = {
    outCSV.flush()
    outCSV.close()
    println(s"*** document count = $count")
  }

  override def init(model: ABPModel) {}
}


class OutputFileWriterFactory extends OutputWriterFactory {
  override def writer(fileRoot: String, settings: WriterSettings): OutputWriter =
    new OutputFileWriter(new File(OutputFileWriterHelper.outputFolder, s"$fileRoot.txt.gz"))
}
