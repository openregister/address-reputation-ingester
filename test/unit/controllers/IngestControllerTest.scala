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

import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.ingester.converter.{Extractor, ExtractorFactory}
import services.ingester.exec.{Continuer, WorkQueue, WorkerFactory}
import services.ingester.model.StateModel
import services.ingester.writers._
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class IngestControllerTest extends FunSuite with MockitoSugar {


  test(
    """
       when an invalid target is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("$%", "abp", 40, "full")
  }

  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("null", "$%", 40, "full")
  }

  test(
    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("null", "abi", 40, ")(")
  }

  def parameterTest(target: String, product: String, epoch: Int, variant: String): Unit = {
    val folder = new File(".")
    val logger = new StubLogger()
    val writerFactory = mock[OutputFileWriterFactory]
    val request = FakeRequest()
    val model = new StateModel(logger, product, epoch, variant, None)

    val ic = new IngestController(folder, logger, null, null, null, null, null)

    intercept[IllegalArgumentException] {
      await(call(ic.ingestTo(target, product, epoch, variant, None, None), request))
    }
  }

  // scalastyle:off
  class context {
    val request = FakeRequest()
    val logger = new StubLogger()
    val ef = mock[ExtractorFactory]
    val ex = mock[Extractor]
    val dbf = mock[OutputDBWriterFactory]
    val fwf = mock[OutputFileWriterFactory]
    val nwf = mock[OutputNullWriterFactory]
    val outputFileWriter = mock[OutputFileWriter]
    val outputDBWriter = mock[OutputDBWriter]
    val outputNullWriter = mock[OutputNullWriter]
    val folder = new File(".")
    val testWorker = new WorkQueue(new StubLogger())
    val workerFactory = new WorkerFactory {
      override def worker = testWorker
    }

    when(fwf.writer(any[StateModel], any[WriterSettings])) thenReturn outputFileWriter
    when(dbf.writer(any[StateModel], any[WriterSettings])) thenReturn outputDBWriter
    when(ef.extractor(any[Continuer], any[StateModel])) thenReturn ex
  }

  test(
    """
      when valid parameters are passed to ingestToDB
      then a successful response is returned
    """) {
    new context {
      val ic = new IngestController(folder, logger, dbf, fwf, nwf, ef, workerFactory)

      val futureResponse = call(ic.ingestTo("db", "abp", 40, "full", Some(1), Some(0)), request)

      val response = await(futureResponse)
      testWorker.awaitCompletion()

      verify(ex, times(1)).extract(any[File], anyObject())
      assert(response.header.status / 100 === 2)
      testWorker.terminate()
    }
  }

  test(
    """
      when valid parameters are passed to ingestToFile
      then a successful response is returned
    """) {
    new context {
      val ic = new IngestController(folder, logger, dbf, fwf, nwf, ef, workerFactory)

      val futureResponse = call(ic.ingestTo("file", "abp", 40, "full", None, None), request)

      val response = await(futureResponse)
      testWorker.awaitCompletion()
      testWorker.terminate()

      verify(ex, times(1)).extract(any[File], anyObject())
      assert(response.header.status / 100 === 2)
      testWorker.terminate()
    }
  }

  test(
    """
      given a StateModel that is in a failed state
      when the inner queueSwitch method is called
      then no task is queued
      and the state model stays in its current state
    """) {
    new context {
      val writerFactory = mock[OutputFileWriterFactory]
      val model = new StateModel(logger, "abp", 40, "full", None)
      val settings = WriterSettings(1, 0)
      val ic = new IngestController(folder, logger, dbf, fwf, nwf, ef, workerFactory)
      model.fail("foo")

      ic.queueIngest(model, settings, writerFactory)

      testWorker.awaitCompletion()

      verify(ex, never).extract(any[File], anyObject())
      assert(logger.size === 2, logger.all.mkString("\n"))
      assert(logger.infos.map(_.message) === List("Info:Ingest was skipped."))
      assert(logger.warns.map(_.message) === List("Warn:foo"))

      testWorker.terminate()
    }
  }
}
