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

package services.ingester.converter

import java.io.File

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import services.ingester.exec.Task
import services.ingester.writers.OutputWriter
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class ExtractorTest extends FunSuite with Matchers with MockitoSugar {


  test("Having no files should not throw any exception") {
    val mockFile = mock[File]
    val logger = new StubLogger
    val task = new Task(logger)
    val dummyOut = mock[OutputWriter]

    when(mockFile.isDirectory) thenReturn true
    when(mockFile.listFiles) thenReturn Array.empty[File]

    task.start {
      new Extractor(task, logger).extract(mockFile, dummyOut)
    }
    task.awaitCompletion()
  }

  test(
    """given a zip archive containing one file,
       Extractor should iterate over the CSV lines it contains
    """) {
    val sample = new File(getClass.getClassLoader.getResource("SX9090-first20.zip").getFile)
    val logger = new StubLogger
    val task = new Task(logger)

    val out = new OutputWriter {
      def output(out: DbAddress) {
        fail("Not expecting output from this sample data file.")
      }
      def close() {}
    }

    task.start {
      new Extractor(task, logger).extract(List(sample), out)
    }
    task.awaitCompletion()

    assert(logger.infos.map(_.message) === List(
      "Info:Reading zip entry SX9090-first20.csv...",
      "Info:Reading from 1 files in {} took {}",
      "Info:First pass obtained 0 BLPUs, 0 DPA UPRNs, 10 streets",
      "Info:First pass complete after {}",
      "Info:Reading zip entry SX9090-first20.csv...",
      "Info:Reading from 1 files in {} took {}",
      "Info:",
      "Info:Finished after {}",
      "Info:Task completed after {}"
    ))
  }

}
