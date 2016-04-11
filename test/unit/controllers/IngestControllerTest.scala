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
import services.ingester.{OutputFileWriter, OutputFileWriterFactory}
import uk.co.hmrc.address.osgb.DbAddress
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
    val ic = new IngestController(folder, logger, null, null, null)

    intercept[IllegalArgumentException] {
      ic.handleIngest(null, product, epoch, variant)
    }
  }

  test(
    """
      when valid paramaters are passed to ingest
      then a successful response is returned
    """) {
    val fwf = mock[OutputFileWriterFactory]
    val ef = mock[ExtractorFactory]
    val exf = mock[TaskFactory]
    val ex = mock[Extractor]
    val folder = new File(".")
    val logger = new StubLogger()

    val stubOut = (dba: DbAddress) => {}
    val task = new Task(logger)
    val outputFileWriter = mock[OutputFileWriter]

    when(fwf.writer(any[File])) thenReturn outputFileWriter
    when(ef.extractor(task)) thenReturn ex
    when(outputFileWriter.csvOut) thenReturn stubOut
    when(exf.task) thenReturn task

    val ic = new IngestController(folder, logger, fwf, ef, exf)

    val result = ic.handleIngest(null, "abp", "40", "full")

    task.awaitCompletion()

    verify(ex, times(1)).extract(any[File], anyObject(), any[StubLogger])
    assert(result.header.status / 100 === 2)
  }
}
