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

import com.typesafe.config.ConfigFactory
import services.ingester.converter.Extractor
import services.ingester.exec.{Task, WorkQueue}
import services.ingester.model.StateModel
import services.ingester.writers.OutputFileWriter
import uk.co.hmrc.logging.Stdout

object Ingester extends App {

  val appStart = System.currentTimeMillis()

  val conf = ConfigFactory.load()
  val home = System.getenv("HOME")

  val osRootFolder = new File(conf.getString("app.files.rootFolder").replace("$HOME", home))
  if (!osRootFolder.exists()) {
    throw new FileNotFoundException(osRootFolder.toString)
  }

  val outputFolder = new File(conf.getString("app.files.outputFolder").replace("$HOME", home))
  outputFolder.mkdirs()

  val model = new StateModel("abp", 0, "output", None, Stdout)
  val outCSV = new OutputFileWriter(model)
  val worker = new WorkQueue(Stdout)

  worker.push(Task("ingesting", {
    continuer => new Extractor(continuer, model).extract(osRootFolder, outCSV)
  }, {
    () => outCSV.close()
  }))

  worker.awaitCompletion()

  val totalTime = (System.currentTimeMillis() - appStart) / 1000
  println(s"Total Execution Time: ${totalTime / 60} mins ${totalTime % 60} secs  ")
}
