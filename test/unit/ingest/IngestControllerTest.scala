/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ingest

import java.io.File

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import controllers.PassThroughAction
import ingest.writers._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.{Continuer, WorkQueue, WorkerFactory}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.writers.{Algorithm, OutputWriter, WriterSettings}
import uk.gov.hmrc.logging.StubLogger

import scala.concurrent.ExecutionContext

@RunWith(classOf[JUnitRunner])
class IngestControllerTest extends FunSuite with MockitoSugar {

  implicit val system = ActorSystem("test")

  implicit def mat: Materializer = ActorMaterializer()

  // scalastyle:off
  class context {
    val request = FakeRequest()
    val logger = new StubLogger()
    val status = new StatusLogger(logger)

    val ingester = mock[Ingester]
    val outputFileWriter = mock[OutputFileWriter]
    val outputESWriter = mock[OutputWriter]
    val outputNullWriter = mock[OutputWriter]

    val ingesterFactory = new StubIngesterFactory(ingester)
    val esFactory = new StubOutputWriterFactory(outputESWriter)
    val fwFactory = new StubOutputWriterFactory(outputFileWriter)
    val nullFactory = new StubOutputWriterFactory(outputNullWriter)

    val folder = new File(".")
    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)

    private val pta = new PassThroughAction
    val ec = scala.concurrent.ExecutionContext.Implicits.global

    val ingestController = new IngestController(pta, folder, esFactory, fwFactory, nullFactory, ingesterFactory, workerFactory, ec)

    def parameterTest(target: String, product: String, epoch: Int, variant: String): Unit = {
      val folder = new File(".")
      val logger = new StubLogger()
      val request = FakeRequest()

      intercept[IllegalArgumentException] {
        await(call(ingestController.doIngestFileTo(target, product, epoch, variant, None, None, None, None, None, None), request))
      }
    }
  }

  test(
    """
       when an invalid target is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("$%", "abp", 40, "full")
    }
  }

  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("null", "$%", 40, "full")
    }
  }

  test(
    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """) {
    new context {
      parameterTest("null", "abi", 40, ")(")
    }
  }

  test(
    """
      when valid parameters are passed to ingestToES
      then a successful response is returned
    """) {
    new context {
      // when
      val response = await(call(ingestController.doIngestFileTo("es", "abp", 40, "full", Some(1), Some(0), None, None, None, None), request))

      // then
      worker.awaitCompletion()
      verify(ingester, times(1)).ingestFrom(new File(folder, "abp/40/full"), outputESWriter)
      assert(response.header.status / 100 === 2)
      worker.terminate()
    }
  }

  test(
    """
      when valid parameters are passed to ingestToFile
      then the output file writer is used to create a new output file
    """) {
    new context {
      // when
      val response = await(call(ingestController.doIngestFileTo("file", "abp", 40, "full", None, None, None, None, None, None), request))

      // then
      worker.awaitCompletion()
      verify(ingester, times(1)).ingestFrom(new File(folder, "abp/40/full"), outputFileWriter)
      assert(response.header.status / 100 === 2)
      worker.terminate()
    }
  }

  test(
    """
      given a StateModel that is in a happy state
      when the inner ingestIfOK method is called
      then the state model index is set
    """) {
    new context {
      val model1 = new StateModel("abp", Some(40), Some("full"))
      val settings = WriterSettings(1, 0, Algorithm.default)
      when(outputESWriter.end(true)) thenReturn false

      // when
      val model3 = ingestController.ingestIfOK(model1, status, settings, "es", new StubContinuer)

      // then
      assert(model3.copy(timestamp = None) === model1)
      verify(ingester).ingestFrom(any[File], anyObject())
      assert(logger.size === 1, logger.all.mkString("\n"))
      assert(logger.infos.map(_.message) === List("Cleaning up the ingester: ok."))

      worker.terminate()
    }
  }

  test(
    """
      given a StateModel that is in a failed state
      when the inner ingestIfOK method is called
      then the state model stays in its current state
    """) {
    new context {
      val model1 = new StateModel("abp", Some(40), Some("full"), hasFailed = true)
      val settings = WriterSettings(1, 0, Algorithm.default)

      // when
      val model2 = ingestController.ingestIfOK(model1, status, settings, "null", new StubContinuer)

      // then
      assert(model2 === model1)
      verify(ingester, never).ingestFrom(any[File], anyObject())
      assert(logger.size === 1, logger.all.mkString("\n"))
      assert(logger.infos.map(_.message) === List("Ingest was skipped."))

      worker.terminate()
    }
  }

  test(
    """
      Settings are correctly hard-limited
    """) {
    import IngestControllerHelper._
    assert(settings(None, None) === WriterSettings(defaultBulkSize, defaultLoopDelay, Algorithm.default))
    assert(settings(Some(7), Some(9)) === WriterSettings(7, 9, Algorithm.default))
    assert(settings(Some(0), None) === WriterSettings(1, defaultLoopDelay, Algorithm.default))
    assert(settings(Some(10001), None) === WriterSettings(10000, defaultLoopDelay, Algorithm.default))
    assert(settings(None, Some(-1)) === WriterSettings(defaultBulkSize, 0, Algorithm.default))
    assert(settings(None, Some(100001)) === WriterSettings(defaultBulkSize, 100000, Algorithm.default))
  }
}

class StubOutputWriterFactory(w: OutputWriter) extends OutputWriterFactory {
  override def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings, ec: ExecutionContext): OutputWriter = w
}

class StubIngesterFactory(i: Ingester) extends IngesterFactory {
  override def ingester(continuer: Continuer, settings: Algorithm, model: StateModel, statusLogger: StatusLogger): Ingester = i
}

class StubWorkerFactory(w: WorkQueue) extends WorkerFactory {
  override def worker = w
}

class StubContinuer extends Continuer {
  override def isBusy: Boolean = true
}
