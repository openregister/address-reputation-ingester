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
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import services.ingester.exec.{Task, TaskFactory}
import services.ingester.writers._
import uk.co.hmrc.address.admin.StoredMetadataItem
import uk.co.hmrc.logging.StubLogger


@RunWith(classOf[JUnitRunner])
class SwitchoverControllerTest extends FunSuite with MockitoSugar {

  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("$%", "40", "12")
  }

  test(
    """
       when an invalid epoch is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("abi", "(*", "12")
  }

  test(
    """
       when an invalid variant is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("abi", "40", "aa")
  }

  def parameterTest(product: String, epoch: String, index: String): Unit = {
    val logger = new StubLogger()
    val storedItem = new StoredMetadataStub()
    val request = FakeRequest()

    val sc = new SwitchoverController(Map("abi" -> storedItem))

    intercept[IllegalArgumentException] {
      sc.handleSwitch(request, product, epoch, index)
    }
  }

  test(
    """
      when valid paramaters are passed to ingest
      then a successful response is returned
    """) {
    val fwf = mock[OutputFileWriterFactory]
    val dbf = mock[OutputDBWriterFactory]
    val exf = mock[TaskFactory]
    val folder = new File(".")
    val logger = new StubLogger()
    val task = new Task(logger)
    val request = FakeRequest()

    when(exf.task) thenReturn task
    val storedItem = new StoredMetadataStub()

    val sc = new SwitchoverController(Map("abp" -> storedItem))

    val result = sc.handleSwitch(request, "abp", "40", "9")

    task.awaitCompletion()

    assert(result.header.status / 100 === 2)
  }
}


class StoredMetadataStub extends StoredMetadataItem {
  private var _value = ""

  override def get: String = _value

  override def set(value: String) {
    _value = value
  }

  override def reset() {}
}
