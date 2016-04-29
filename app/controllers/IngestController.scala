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
import services.ingester.exec.{Task, WorkerFactory}
import services.ingester.model.ABPModel
import services.ingester.writers._
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
  new OutputNullWriterFactory,
  new ExtractorFactory,
  new WorkerFactory())


class IngestController(rootFolder: File,
                       logger: SimpleLogger,
                       dbWriterFactory: OutputDBWriterFactory,
                       fileWriterFactory: OutputFileWriterFactory,
                       nullWriterFactory: OutputNullWriterFactory,
                       extractorFactory: ExtractorFactory,
                       workerFactory: WorkerFactory
                      ) extends BaseController {

  def ingestToDB(product: String, epoch: Int, variant: String,
                 bulkSizeStr: Option[Int], loopDelayStr: Option[Int]): Action[AnyContent] = Action {
    request =>
      val bulkSize = bulkSizeStr getOrElse 1
      val loopDelay = loopDelayStr getOrElse 0
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(constrainRange(bulkSize, 1, 10000), constrainRange(loopDelay, 0, 100000))
      val model = new ABPModel(product, epoch, variant, None, logger)
      handleIngest(model, settings, dbWriterFactory)
  }

  def ingestToFile(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(1, 0)
      val model = new ABPModel(product, epoch, variant, None, logger)
      handleIngest(model, settings, fileWriterFactory)
  }

  def ingestToNull(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    request =>
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val settings = WriterSettings(1, 0)
      val model = new ABPModel(product, epoch, variant, None, logger)
      handleIngest(model, settings, nullWriterFactory)
  }

  private[controllers] def handleIngest(model: ABPModel,
                                        settings: WriterSettings,
                                        writerFactory: OutputWriterFactory): Result = {
    val qualifiedDir = new File(rootFolder, model.pathSegment)

    val writer = writerFactory.writer(model.collectionBaseName, settings)

    workerFactory.worker.push(
      Task(s"ingesting ${model.pathSegment}", {
        continuer =>
          extractorFactory.extractor(continuer, logger).extract(qualifiedDir, writer)
      },
        () => {
          logger.info("cleaning up extractor")
          writer.close()
        })
    )

    Accepted(s"Ingestion has started for ${model.pathSegment}")
  }
}
