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

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import services.ingester.converter.{Extractor, ExtractorFactory}
import services.ingester.exec.{Task, TaskFactory}
import services.ingester.writers._
import uk.co.hmrc.logging.StubLogger

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

    val ic = new IngestController(folder, logger, null, null, null, null)

    intercept[IllegalArgumentException] {
      ic.handleIngest(null, product, epoch, variant, WriterSettings.default, writerFactory)
    }
  }

  test(
    """
      when valid paramaters are passed to ingest
      then a successful response is returned
    """) {
    val fwf = mock[OutputFileWriterFactory]
    val dbf = mock[OutputDBWriterFactory]
    val ef = mock[ExtractorFactory]
    val exf = mock[TaskFactory]
    val ex = mock[Extractor]
    val folder = new File(".")
    val logger = new StubLogger()
    val task = new Task(logger)
    val outputFileWriter = mock[OutputFileWriter]
    val outputDBWriter = mock[OutputDBWriter]

    when(fwf.writer(anyString, any[WriterSettings])) thenReturn outputFileWriter
    when(dbf.writer(anyString, any[WriterSettings])) thenReturn outputDBWriter
    when(ef.extractor(task, logger)) thenReturn ex
    when(exf.task) thenReturn task

    val ic = new IngestController(folder, logger, dbf, fwf, ef, exf)

    val result = ic.handleIngest(null, "abp", "40", "full", WriterSettings.default, fwf)

    task.awaitCompletion()

    verify(ex, times(1)).extract(any[File], anyObject())
    assert(result.header.status / 100 === 2)
  }
}
