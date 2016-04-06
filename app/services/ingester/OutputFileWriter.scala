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

package services.ingester

import java.io.{OutputStreamWriter, _}
import java.util.zip.GZIPOutputStream

import uk.co.hmrc.address.osgb.DbAddress


class OutputFileWriter(outputFile: File) {

  private val bufSize = 32 * 1024
  private val outfile = new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(outputFile), bufSize))
  private val outCSV = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outfile), bufSize))

  val csvOut = (out: DbAddress) => {
    // scalastyle:off
    outCSV.println(out.toString)
  }

  def close() {
    outCSV.flush()
    outCSV.close()
  }

}


class OutputFileWriterFactory {
  def writer(outputFile: File): OutputFileWriter = new OutputFileWriter(outputFile)
}