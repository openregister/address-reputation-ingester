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

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoCollection, MongoDB}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.http.HeaderNames.{WWW_AUTHENTICATE => _}
import play.api.mvc._
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.audit.AuditClient
import services.es.ElasticsearchHelper
import services.exec.{WorkQueue, WorkerFactory}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.admin.StoredMetadataItem
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger
import uk.gov.hmrc.secure.{Salt, Scrambled}

import scala.concurrent.Future


@RunWith(classOf[JUnitRunner])
class SwitchoverControllerTest extends FunSuite with MockitoSugar {

  // password="password"
  val authConfig = BasicAuthenticationFilterConfiguration("address-reputation-ingester", true, "admin",
    Scrambled("StfavBn0QDn042NBdycLMPogG+jE6uCE"), Salt("RaTwtD0w8rOweSGf"))

  val pta = new PassThroughAction

  test(
    """
       when an invalid product is passed to ingest
       then an exception is thrown
    """) {
    parameterTest("db", "$%", 40, "2001-02-03-04-05")
  }

  def parameterTest(target: String, product: String, epoch: Int, timestamp: String): Unit = {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val testWorker = new WorkQueue(status)
    val auditClient = mock[AuditClient]
    val elasticSearchHelper = mock[ElasticsearchHelper]

    val workerFactory = new WorkerFactory {
      override def worker = testWorker
    }
    val store = mock[MongoSystemMetadataStore]
    when(store.addressBaseCollectionItem(anyString)) thenThrow new IllegalArgumentException()

    val mongo = mock[CasbahMongoConnection]
    val request = FakeRequest()

    val switchoverController = new SwitchoverController(pta, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
      scala.concurrent.ExecutionContext.Implicits.global)

    intercept[IllegalArgumentException] {
      await(call(switchoverController.doSwitchTo(target, product, epoch, timestamp), request))
    }
  }

  class Context {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val auditClient = mock[AuditClient]
    val elasticSearchHelper = mock[ElasticsearchHelper]
    val testWorker = new WorkQueue(status)
    val workerFactory = new WorkerFactory {
      override def worker = testWorker
    }

    val storedItem = new StoredMetadataStub()
    val store = mock[MongoSystemMetadataStore]
    when(store.addressBaseCollectionItem("abp")) thenReturn storedItem

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
      and an audit message is logged that describes the change
    """) {
    new Context {
      when(db.collectionExists("abp_40_2001-02-03-04-05")) thenReturn true
      when(db.apply("abp_40_2001-02-03-04-05")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn Some(MongoDBObject("completedAt" -> 0L))
      when(collection.name) thenReturn "abp_40_2001-02-03-04-05"

      val sc = new SwitchoverController(pta, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
        scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("db", "abp", 40, "2001-02-03-04-05"), request))

      assert(response.header.status / 100 === 2)
      testWorker.awaitCompletion()
      testWorker.terminate()

      assert(storedItem.get === "abp_40_2001-02-03-04-05")

      verify(auditClient).succeeded(Map("product" -> "abp", "epoch" -> "40", "newCollection" -> "abp_40_2001-02-03-04-05"))
    }
  }

  test(
    """
      given that the intended collection does not exist
      when valid parameters are passed to ingest
      then the switch-over fails due to missing the collection
      and the stored metadata item for the product in question is left unchanged
    """) {
    new Context {
      when(db.collectionExists(anyString)) thenReturn false

      val sc = new SwitchoverController(pta, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
        scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("db", "abp", 40, "2001-02-03-04-05"), request))

      testWorker.awaitCompletion()
      testWorker.terminate()

      assert(storedItem.get === "the initial value")
      assert(logger.warns.size === 2, logger.all)
      assert(logger.warns.head.message === "Warn:abp_40_2001-02-03-04-05: collection was not found")
    }
  }

  test(
    """
      given that the intended collection exists but does not contain the metadata containedAt
      when valid parameters are passed to ingest
      then the switch-over fails due to conflict
      and the stored metadata item for the product in question is left unchanged
    """) {
    new Context {
      when(db.collectionExists("abp_40_2001-02-03-04-05")) thenReturn true
      when(db.apply("abp_40_2001-02-03-04-05")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn None
      when(collection.name) thenReturn "abp_40_2001-02-03-04-05"

      val sc = new SwitchoverController(pta, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
        scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("db", "abp", 40, "2001-02-03-04-05"), request))

      testWorker.awaitCompletion()
      testWorker.terminate()

      assert(response.header.status === 202)
      assert(storedItem.get === "the initial value")
      assert(logger.warns.size === 2, logger.all)
      assert(logger.warns.head.message === "Warn:abp_40_2001-02-03-04-05: collection is still being written")
    }
  }

  test(
    """
      when a request is received without valid basic-auth headers
      then the response is 401
      and the stored metadata item for the product in question is left unchanged
    """) {
    new Context {
      when(db.collectionExists("abp_40_ts9")) thenReturn true
      when(db.apply("abp_40_ts9")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn None
      when(collection.name) thenReturn "abp_40_ts9"

      val sc = new SwitchoverController(new FailAuthAction, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
        scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("db", "abp", 40, "2001-02-03-04-05"), request))

      testWorker.awaitCompletion()
      testWorker.terminate()

      assert(response.header.status === 401)
      assert(storedItem.get === "the initial value")
    }
  }

  test(
    """
      given a StateModel that is in a failed state
      when the inner switchIfOK method is called
      then no task is performed
      and the state model stays in its current state
    """) {
    new Context {
      when(db.collectionExists("abp_40_ts9")) thenReturn true
      when(db.apply("abp_40_ts9")) thenReturn collection
      when(collection.findOneByID("metadata")) thenReturn None
      when(collection.name) thenReturn "abp_40_ts9"

      val sc = new SwitchoverController(pta, status, workerFactory, mongo, store, auditClient, elasticSearchHelper,
        scala.concurrent.ExecutionContext.Implicits.global)
      val model1 = new StateModel("abp", 40, Some("full"), Some("2001-02-03-04-05"), hasFailed = true)

      val model2 = sc.switchIfOK(model1)

      testWorker.awaitCompletion()

      assert(model2 === model1)
      assert(storedItem.get === "the initial value")
      assert(logger.size === 1, logger.all.mkString("\n"))
      assert(logger.infos.map(_.message) === List("Info:Switchover was skipped."))

      testWorker.terminate()
    }
  }
}


class StoredMetadataStub(private var _value: String = "the initial value") extends StoredMetadataItem {

  override def get: String = _value

  override def set(value: String) {
    _value = value
  }

  override def reset() {}
}


class FailAuthAction extends ActionBuilder[Request] with ActionFilter[Request] {

  val fail = Some(Results.Unauthorized.withHeaders(WWW_AUTHENTICATE -> "some realm"))

  def filter[A](request: Request[A]): Future[Option[Result]] = Future.successful(fail)
}
