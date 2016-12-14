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

package ingest

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import com.google.inject.Inject
import controllers.ControllerConfig
import controllers.SimpleValidator._
import ingest.algorithm.AlgorithmSettings
import ingest.writers._
import play.api.mvc.{Action, AnyContent}
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.writers.{Algorithm, WriterSettings}
import uk.gov.hmrc.play.microservice.controller.BaseController


object IngestControllerHelper {
  val allowedTargets = Set("db", "es", "file", "null")
  val defaultBulkSize = 1000
  val defaultLoopDelay = 0

  def settings(opBulkSize: Option[Int], opLoopDelay: Option[Int], algorithm: Algorithm = Algorithm.default): WriterSettings = {
    val bulkSize = opBulkSize getOrElse defaultBulkSize
    val loopDelay = opLoopDelay getOrElse defaultLoopDelay
    WriterSettings(constrainRange(bulkSize, 1, 10000), constrainRange(loopDelay, 0, 100000), algorithm)
  }
}


class IngestController @Inject()(cc: ControllerConfig,
                                 esWriterFactory: OutputESWriterFactory,
                                 fileWriterFactory: OutputFileWriterFactory,
                                 nullWriterFactory: OutputNullWriterFactory,
                                 ingesterFactory: IngesterFactory,
                                 worker: WorkQueue,
                                 statusLogger: StatusLogger
                                ) extends BaseController {

  def doIngestFileTo(
                      target: String, product: String, epoch: Int, variant: String,
                      bulkSize: Option[Int], loopDelay: Option[Int],
                      forceChange: Option[Boolean],
                      include: Option[String],
                      prefer: Option[String],
                      streetFilter: Option[Int]
                    ): Action[AnyContent] = Action {
    request =>
      require(IngestControllerHelper.allowedTargets.contains(target))
      require(isAlphaNumeric(product))
      require(isAlphaNumeric(variant))

      val model = new StateModel(product, Some(epoch), Some(variant), forceChange = forceChange getOrElse false)
      val algorithmSettings = AlgorithmSettings(include, prefer, streetFilter)
      val settings = IngestControllerHelper.settings(bulkSize, loopDelay, algorithmSettings)

      worker.push(
        s"ingesting to $target ${model.pathSegment}${model.forceChangeString}",
        continuer => ingestIfOK(model, statusLogger, settings, target, continuer)
      )

      Accepted(s"Ingestion has started for ${model.pathSegment}${model.forceChangeString}")
  }

  def ingestIfOK(model: StateModel,
                 status: StatusLogger,
                 writerSettings: WriterSettings,
                 target: String,
                 continuer: Continuer): StateModel = {
    if (!model.hasFailed) {
      val writerFactory = pickWriter(target)
      val modelWithTimestamp = model.withNewTimestamp
      ingest(modelWithTimestamp, status, writerSettings, writerFactory, continuer)
    } else {
      status.info("Ingest was skipped.")
      model // unchanged
    }
  }

  private def ingest(model: StateModel,
                     status: StatusLogger,
                     writerSettings: WriterSettings,
                     writerFactory: OutputWriterFactory,
                     continuer: Continuer): StateModel = {

    val dataLoc = model.productName match {
      case "test" => new File(cannedDataLoc)
      case _ => new File(cc.downloadFolder, model.pathSegment)
    }
    val writer = writerFactory.writer(model, writerSettings)
    var result = model
    var ingestFailed = true
    try {
      ingestFailed = ingesterFactory.ingester(continuer, writerSettings.algorithm, model).ingestFrom(dataLoc, writer)
    } catch {
      case e: Exception =>
        status.warn(e.getMessage)
        status.tee.warn(e.getMessage, e)
    } finally {
      if (!ingestFailed) {
        val endFailed = writer.end(continuer.isBusy)
        val ok = if (endFailed) "failed" else "ok"
        status.info(s"Cleaning up the ingester: $ok.")
        result = result.copy(hasFailed = endFailed)
      }
    }
    if (ingestFailed) result.copy(hasFailed = true) else result
  }

  private def cannedDataLoc() = {
    Option(getClass.getClassLoader.getResource("data/canned.zip")).map { url =>
      url.getProtocol match {
        case "file" => url.getFile
        case "jar" => {
          val tempFile = Files.createTempFile("ari-canned", ".zip")
          Files.copy(url.openStream, tempFile, StandardCopyOption.REPLACE_EXISTING)
          tempFile.toString
        }
        case _ => "data/canned.zip"
      }
    }.getOrElse("data/canned.zip")
  }

  private def withCloseable[T, C <: AutoCloseable](closeable: C)(block: C => T) = {
    try
      block(closeable)
    finally
      closeable.close()
  }

  private def pickWriter(target: String) = {
    target match {
      case "es" => esWriterFactory
      case "file" => fileWriterFactory
      case "null" => nullWriterFactory
      case _ =>
        throw new IllegalArgumentException(target + " not supported.")
    }
  }
}
