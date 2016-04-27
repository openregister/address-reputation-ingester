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

import com.mongodb.casbah.{MongoCollection, MongoDB}
import com.mongodb.casbah.commons.MongoDBObject
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.test.FakeRequest
import play.api.test.Helpers._
import uk.co.hmrc.address.admin.StoredMetadataItem
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
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
    val mongo = mock[CasbahMongoConnection]
    val request = FakeRequest()

    val sc = new SwitchoverController(mongo, Map("abi" -> storedItem))

    intercept[IllegalArgumentException] {
      sc.handleSwitch(request, product, epoch, index)
    }
  }

  class context {
    val storedItem = new StoredMetadataStub()
    val request = FakeRequest()
    val mongo = mock[CasbahMongoConnection]
    val db = mock[MongoDB]
    val collection = mock[MongoCollection]
    when(mongo.getConfiguredDb) thenReturn db
  }

  test(
    """
      given that the intended collection exists and contains the containedAt metadata
      when valid parameters are passed to ingest
      then a successful response is returned
      and the stored metadata item for the product in question is set to the new collection name
    """) {
    println("********** SCT1 **********")
    new context {
      when(db.collectionExists("abp_40_9")) thenReturn true
      when(db.apply("abp_40_9")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn Some(MongoDBObject())

      val sc = new SwitchoverController(mongo, Map("abp" -> storedItem))
      val futureResponse = call(sc.switchTo("abp", "40", "9"), request)

      val response = await(futureResponse)
      assert(response.header.status / 100 === 2)
      assert(storedItem.get === "abp_40_9")
    }
  }

  test(
    """
      given that the intended collection does not exist
      when valid parameters are passed to ingest
      then a bad-request response is returned
      and the stored metadata item for the product in question is left unchanged
    """) {
    println("********** SCT2 **********")
    new context {
      when(db.collectionExists(anyString)) thenReturn false

      val sc = new SwitchoverController(mongo, Map("abp" -> storedItem))
      val futureResponse = call(sc.switchTo("abp", "40", "9"), request)

      val response = await(futureResponse)
      assert(response.header.status === 400)
      assert(storedItem.get === "the initial value")
    }
  }

  test(
    """
      given that the intended collection exists but does not contain the metadata containedAt
      when valid parameters are passed to ingest
      then a conflict response is returned
      and the stored metadata item for the product in question is left unchanged
    """) {
    println("********** SCT3 **********")
    new context {
      when(db.collectionExists("abp_40_9")) thenReturn true
      when(db.apply("abp_40_9")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn None

      val sc = new SwitchoverController(mongo, Map("abp" -> storedItem))
      val futureResponse = call(sc.switchTo("abp", "40", "9"), request)

      val response = await(futureResponse)
      assert(response.header.status === 409)
      assert(storedItem.get === "the initial value")
    }
  }
}


class StoredMetadataStub extends StoredMetadataItem {
  private var _value = "the initial value"

  override def get: String = _value

  override def set(value: String) {
    _value = value
  }

  override def reset() {}
}
