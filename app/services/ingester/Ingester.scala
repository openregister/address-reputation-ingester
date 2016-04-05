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

import java.io._
import java.util.zip.GZIPOutputStream

import com.typesafe.config.ConfigFactory
import services.ingester.converter.Extractor
import uk.co.hmrc.address.osgb.DbAddress

object Ingester extends App {

  val appStart = System.currentTimeMillis()

  val conf = ConfigFactory.load()
  val home = System.getenv("HOME")

  val osRootFolder = new File(conf.getString("app.files.rootFolder").replace("$HOME", home))
  if (!osRootFolder.exists()) {
    throw new FileNotFoundException(osRootFolder.toString)
  }

  val tmpZipfilesHome = conf.getString("app.files.tempFolder")

  val bufSize = 32 * 1024
  val outfile = new GZIPOutputStream(new FileOutputStream(new File(osRootFolder, "output.csv.gz")))
  val outCSV = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outfile), bufSize))

  val csvOut = (out: DbAddress) => {
    // scalastyle:off
    outCSV.println(out.toString)
  }

  val result = Extractor.extract(osRootFolder, csvOut)
  println("Result: " + result.toString)

  outCSV.flush()
  outCSV.close()

  val totalTime = (System.currentTimeMillis() - appStart) / 1000
  println(s"Total Execution Time: ${totalTime / 60} mins ${totalTime % 60} secs  ")
}