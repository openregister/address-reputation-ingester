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

package controllers

import java.io.{File, FileNotFoundException}

import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent, Request, Result}
import services.ingester.OutputFileWriterFactory
import services.ingester.converter.ExtractorFactory
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController

object IngestControllerHelper {
  val home = System.getenv("HOME")
  val rootFolder = new File(mustGetConfigString(current.mode, current.configuration, "app.files.rootFolder").replace("$HOME", home))
  if (!rootFolder.exists()) {
    throw new FileNotFoundException(rootFolder.toString)
  }
}

object IngestController extends IngestController(IngestControllerHelper.rootFolder,
  new LoggerFacade(Logger.logger), new OutputFileWriterFactory, new ExtractorFactory)


class IngestController(rootFolder: File, logger: SimpleLogger, fileWriterFactory: OutputFileWriterFactory,
                       extractorFactory: ExtractorFactory) extends BaseController {

  val alphaNumPattern = "[a-z0-9]+".r

  def ingest(product: String, epoch: String, variant: String): Action[AnyContent] = Action {
    request =>
      handleIngest(request, product, epoch, variant)
  }

  def handleIngest(request: Request[AnyContent], product: String, epoch: String, variant: String): Result = {
    require(alphaNumPattern.pattern.matcher(product).matches())
    require(alphaNumPattern.pattern.matcher(epoch).matches())
    require(alphaNumPattern.pattern.matcher(variant).matches())

    val qualifiedDir = new File(rootFolder, s"$product/$epoch/$variant")
    val outputFile = new File(qualifiedDir, "output.txt.gz")

    val fw = fileWriterFactory.writer(outputFile)

    val timer = new DiagnosticTimer
    val result = extractorFactory.extractor.extract(qualifiedDir, fw.csvOut)
    logger.info(timer.toString)

    fw.close()
    NoContent
  }

}
