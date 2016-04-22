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
import services.ingester.exec.{Worker, WorkerFactory}
import services.ingester.writers._
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class IngestControllerTest extends FunSuite with MockitoSugar {


  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("$%", "40", "full")
  }

  test(
    """
       when an invalid epoch is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("abi", "(*", "full")
  }

  test(
    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("abi", "40", ")(")
  }

  def parameterTest(product: String, epoch: String, variant: String): Unit = {
    val folder = new File(".")
    val logger = new StubLogger()
    val writerFactory = mock[OutputFileWriterFactory]
    val request = FakeRequest()

    val ic = new IngestController(folder, logger, null, null, null, null)

    intercept[IllegalArgumentException] {
      ic.handleIngest(request, product, epoch, variant, WriterSettings.default, writerFactory)
    }
  }

  // scalastyle:off
  class context {
    val request = FakeRequest()
    val logger = new StubLogger()
    val worker = new Worker(logger)
    val ef = mock[ExtractorFactory]
    val exf = mock[WorkerFactory]
    val ex = mock[Extractor]
    val dbf = mock[OutputDBWriterFactory]
    val fwf = mock[OutputFileWriterFactory]
    val outputFileWriter = mock[OutputFileWriter]
    val outputDBWriter = mock[OutputDBWriter]
    val folder = new File(".")

    when(fwf.writer(anyString, any[WriterSettings])) thenReturn outputFileWriter
    when(dbf.writer(anyString, any[WriterSettings])) thenReturn outputDBWriter
    when(ef.extractor(worker, logger)) thenReturn ex
    when(exf.worker) thenReturn worker
  }

  test(
    """
      when valid paramaters are passed to ingestToDB
      then a successful response is returned
    """) {
    new context {
      val ic = new IngestController(folder, logger, dbf, fwf, ef, exf)

      val futureResult = call(ic.ingestToDB("abp", "40", "full", "1", "0"), request)

      worker.awaitCompletion()
      val result = await(futureResult)

      verify(ex, times(1)).extract(any[File], anyObject())
      assert(result.header.status / 100 === 2)
    }
  }

  test(
    """
      when valid paramaters are passed to ingestToFile
      then a successful response is returned
    """) {
    new context {
      val ic = new IngestController(folder, logger, dbf, fwf, ef, exf)

      val futureResult = call(ic.ingestToFile("abp", "40", "full"), request)

      worker.awaitCompletion()
      val result = await(futureResult)

      verify(ex, times(1)).extract(any[File], anyObject())
      assert(result.header.status / 100 === 2)
    }
  }
}
