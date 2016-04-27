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
import controllers.SimpleValidator._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent, Request, Result}
import services.ingester.converter.ExtractorFactory
import services.ingester.exec.{Task, WorkerFactory}
import services.ingester.writers.{OutputDBWriterFactory, OutputFileWriterFactory, OutputWriterFactory, WriterSettings}
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object IngestControllerHelper {
  val home = System.getenv("HOME")
  val rootFolder = new File(mustGetConfigString(current.mode, current.configuration, "app.files.rootFolder").replace("$HOME", home))
  if (!rootFolder.exists()) {
    throw new FileNotFoundException(rootFolder.toString)
  }
}


object IngestController extends IngestController(
  IngestControllerHelper.rootFolder,
  new LoggerFacade(Logger.logger),
  new OutputDBWriterFactory,
  new OutputFileWriterFactory,
  new ExtractorFactory,
  new WorkerFactory())


class IngestController(rootFolder: File,
                       logger: SimpleLogger,
                       dbWriterFactory: OutputDBWriterFactory,
                       fileWriterFactory: OutputFileWriterFactory,
                       extractorFactory: ExtractorFactory,
                       workerFactory: WorkerFactory
                      ) extends BaseController {

  def ingestToDB(product: String, epoch: String, variant: String,
                 bulkSizeStr: String, loopDelayStr: String): Action[AnyContent] = Action {
    request =>
      val bulkSize = defaultTo(bulkSizeStr, "1")
      val loopDelay = defaultTo(loopDelayStr, "0")

      require(isNumeric(bulkSize))
      require(isNumeric(loopDelay))

      val settings = WriterSettings(constrainRange(bulkSize.toInt, 1, 10000), constrainRange(loopDelay.toInt, 0, 100000))
      handleIngest(request, product, epoch, variant, settings, dbWriterFactory)
  }

  def ingestToFile(product: String, epoch: String, variant: String): Action[AnyContent] = Action {
    request =>
      val settings = WriterSettings(1, 0)
      handleIngest(request, product, epoch, variant, settings, fileWriterFactory)
  }

  private[controllers] def handleIngest(request: Request[AnyContent],
                                        product: String, epoch: String, variant: String,
                                        settings: WriterSettings,
                                        writerFactory: OutputWriterFactory): Result = {
    require(isAlphaNumeric(product))
    require(isAlphaNumeric(epoch))
    require(isAlphaNumeric(variant))

    val qualifiedDir = new File(rootFolder, s"$product/$epoch/$variant")

    val writer = writerFactory.writer(s"${product}_${epoch}", settings)

    val worker = workerFactory.worker
    val status = worker.push(
      Task(s"ingesting $product/$epoch/$variant", {
        continuer =>
          extractorFactory.extractor(continuer, logger).extract(qualifiedDir, writer)
      },
        () => {
          logger.info("cleaning up extractor")
          writer.close()
        })
    )

    if (status) Ok(s"Ingestion has started for $product/$epoch/$variant")
    else Conflict(worker.status)
  }
}
