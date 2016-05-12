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
import uk.gov.hmrc.play.microservice.controller.BaseController


object IngestControllerHelper {
  def isSupportedTarget(target: String): Boolean = Set("db", "file", "null").contains(target)

  def settings(opBulkSize: Option[Int], opLoopDelay: Option[Int]): WriterSettings = {
    val bulkSize = opBulkSize getOrElse 1
    val loopDelay = opLoopDelay getOrElse 0
    WriterSettings(constrainRange(bulkSize, 1, 10000), constrainRange(loopDelay, 0, 100000))
  }
}


object IngestController extends IngestController(
  ControllerConfig.downloadFolder,
  new OutputDBWriterFactory,
  new OutputFileWriterFactory,
  new OutputNullWriterFactory,
  new IngesterFactory,
  ControllerConfig.workerFactory)

class IngestController(unpackedFolder: File,
                       dbWriterFactory: OutputDBWriterFactory,
                       fileWriterFactory: OutputFileWriterFactory,
                       nullWriterFactory: OutputNullWriterFactory,
                       ingesterFactory: IngesterFactory,
                       workerFactory: WorkerFactory
                      ) extends BaseController {

  def doIngestTo(target: String, product: String, epoch: Int, variant: String,
                 bulkSize: Option[Int], loopDelay: Option[Int],
                 forceChange: Option[Boolean]): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.isSupportedTarget(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = IngestControllerHelper.settings(bulkSize, loopDelay)
      val model = new StateModel(product, epoch, variant, forceChange = forceChange getOrElse false)

      val worker = workerFactory.worker
      worker.push(
        s"ingesting ${model.pathSegment}", {
          continuer =>
            ingestIfOK(model, worker.statusLogger, settings, target, continuer)
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
      writer.begin()
      ingesterFactory.ingester(continuer, model, status).ingest(qualifiedDir, writer)
    } finally {
      status.info("Cleaning up the ingester.")
      writer.end(continuer.isBusy)
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
