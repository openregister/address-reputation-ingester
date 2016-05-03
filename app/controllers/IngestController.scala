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
import play.api.mvc.{Action, AnyContent, Result}
import services.ingester.converter.ExtractorFactory
import services.ingester.exec.{Continuer, WorkerFactory}
import services.ingester.model.StateModel
import services.ingester.writers._
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}
import uk.gov.hmrc.play.microservice.controller.BaseController


object IngestControllerHelper {
  val downloadFolder = new File(replaceHome(mustGetConfigString(current.mode, current.configuration, "app.files.downloadFolder")))
  if (!downloadFolder.exists()) {
    throw new FileNotFoundException(downloadFolder.toString)
  }
}


object IngestController extends IngestController(
  IngestControllerHelper.downloadFolder,
  new LoggerFacade(Logger.logger),
  new OutputDBWriterFactory,
  new OutputFileWriterFactory,
  new OutputNullWriterFactory,
  new ExtractorFactory,
  new WorkerFactory())


class IngestController(downloadFolder: File,
                       logger: SimpleLogger,
                       dbWriterFactory: OutputDBWriterFactory,
                       fileWriterFactory: OutputFileWriterFactory,
                       nullWriterFactory: OutputNullWriterFactory,
                       extractorFactory: ExtractorFactory,
                       workerFactory: WorkerFactory
                      ) extends BaseController {

  def ingestTo(target: String, product: String, epoch: Int, variant: String,
               bulkSizeStr: Option[Int], loopDelayStr: Option[Int]): Action[AnyContent] = Action {
    request =>
      val bulkSize = bulkSizeStr getOrElse 1
      val loopDelay = loopDelayStr getOrElse 0
      require(isAlphaNumeric(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val writerFactory = target match {
        case "db" => dbWriterFactory
        case "file" => fileWriterFactory
        case "null" => nullWriterFactory
        case _ =>
          throw new IllegalArgumentException(target + " no supported.")
      }

      val settings = WriterSettings(constrainRange(bulkSize, 1, 10000), constrainRange(loopDelay, 0, 100000))
      val model = new StateModel(logger, product, epoch, variant, None)
      queueIngest(model, settings, writerFactory)
  }

  private[controllers] def queueIngest(model: StateModel,
                                       settings: WriterSettings,
                                       writerFactory: OutputWriterFactory): Result = {
    val qualifiedDir = new File(downloadFolder, model.pathSegment)

    workerFactory.worker.push(
      s"ingesting ${model.pathSegment}", model, {
        continuer =>
          if (!model.hasFailed) {
            ingest(model, settings, writerFactory, qualifiedDir, continuer)
          } else {
            model.statusLogger.info("Ingest was skipped.")
          }
      }
    )

    Accepted(s"Ingestion has started for ${model.pathSegment}")
  }

  private def ingest(model: StateModel, settings: WriterSettings, writerFactory: OutputWriterFactory, qualifiedDir: File, continuer: Continuer): Unit = {
    val writer = writerFactory.writer(model, settings)
    try {
      extractorFactory.extractor(continuer, model).extract(qualifiedDir, writer)
    } finally {
      logger.info("cleaning up extractor")
      writer.close()
    }
  }
}
