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

import java.io.File

import controllers.SimpleValidator._
import play.api.mvc.{Action, AnyContent}
import services.exec.{Continuer, WorkerFactory}
import services.ingest.IngesterFactory
import services.model.{StateModel, StatusLogger}
import services.writers._
import uk.co.hmrc.logging.SimpleLogger
import uk.gov.hmrc.play.microservice.controller.BaseController


object IngestControllerHelper {
  def isSupportedTarget(target: String): Boolean = Set("db", "file", "null").contains(target)
}


object IngestController extends IngestController(
  ControllerConfig.unpackFolder,
  ControllerConfig.logger,
  new OutputDBWriterFactory,
  new OutputFileWriterFactory,
  new OutputNullWriterFactory,
  new IngesterFactory,
  ControllerConfig.workerFactory)


class IngestController(unpackedFolder: File,
                       logger: SimpleLogger,
                       dbWriterFactory: OutputDBWriterFactory,
                       fileWriterFactory: OutputFileWriterFactory,
                       nullWriterFactory: OutputNullWriterFactory,
                       ingestorFactory: IngesterFactory,
                       workerFactory: WorkerFactory
                      ) extends BaseController {

  def doIngestTo(target: String, product: String, epoch: Int, variant: String,
                 bulkSizeStr: Option[Int], loopDelayStr: Option[Int]): Action[AnyContent] = Action {
    request =>
      val bulkSize = bulkSizeStr getOrElse 1
      val loopDelay = loopDelayStr getOrElse 0
      require(IngestControllerHelper.isSupportedTarget(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(constrainRange(bulkSize, 1, 10000), constrainRange(loopDelay, 0, 100000))
      val model = new StateModel(product, epoch, variant, None)
      val status = new StatusLogger(logger)

      workerFactory.worker.push(
        s"ingesting ${model.pathSegment}", status, {
          continuer =>
            ingestIfOK(model, status, settings, target, continuer)
        }
      )

      Accepted(s"Ingestion has started for ${model.pathSegment}")
  }

  private[controllers] def ingestIfOK(model: StateModel,
                                      status: StatusLogger,
                                      settings: WriterSettings,
                                      target: String,
                                      continuer: Continuer): StateModel = {
    if (!model.hasFailed) {
      val writerFactory = pickWriter(target)
      ingest(model, status, settings, writerFactory, continuer)
    } else {
      status.info("Ingest was skipped.")
      model // unchanged
    }
  }

  private def ingest(model: StateModel,
                     status: StatusLogger,
                     settings: WriterSettings,
                     writerFactory: OutputWriterFactory,
                     continuer: Continuer): StateModel = {

    val qualifiedDir = new File(unpackedFolder, model.pathSegment)
    val writer = writerFactory.writer(model, status, settings)
    try {
      ingestorFactory.ingester(continuer, model, status).ingest(qualifiedDir, writer)
    } finally {
      logger.info("Cleaning up the ingester.")
      writer.close()
    }
  }

  private def pickWriter(target: String) = {
    target match {
      case "db" => dbWriterFactory
      case "file" => fileWriterFactory
      case "null" => nullWriterFactory
      case _ =>
        throw new IllegalArgumentException(target + " not supported.")
    }
  }
}
