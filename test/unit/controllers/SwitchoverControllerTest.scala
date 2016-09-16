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

import java.util.Date

import ingest.StubWorkerFactory
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.http.HeaderNames.{WWW_AUTHENTICATE => _}
import play.api.mvc._
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.DbFacade
import services.audit.AuditClient
import services.es.IndexMetadata
import services.exec.WorkQueue
import services.model.{StateModel, StatusLogger}
import services.mongo.{CollectionMetadataItem, CollectionName}
import uk.co.hmrc.address.admin.StoredMetadataItem
import uk.co.hmrc.logging.StubLogger
import uk.gov.hmrc.secure.{Salt, Scrambled}

import scala.concurrent.Future


@RunWith(classOf[JUnitRunner])
class SwitchoverControllerTest extends FunSuite with MockitoSugar {

  val abp_40_ts12345 = CollectionName("abp_40_2001-02-03-04-05").get

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
    val auditClient = mock[AuditClient]
    val indexMetadata = mock[IndexMetadata]

    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)

    val request = FakeRequest()
    val dbFacade = mock[DbFacade]

    val switchoverController = new SwitchoverController(pta, status, workerFactory, dbFacade, auditClient,
      target, scala.concurrent.ExecutionContext.Implicits.global)

    intercept[IllegalArgumentException] {
      await(call(switchoverController.doSwitchTo(product, epoch, timestamp), request))
    }
  }

  class Context {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val auditClient = mock[AuditClient]
    val indexMetadata = mock[IndexMetadata]

    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)

    val request = FakeRequest()
    val dbFacade = mock[DbFacade]
  }

  test(
    """
      given that the intended collection exists and contains the containedAt metadata
      when valid parameters are passed to doSwitchTo
      then a successful response is returned
      and the stored metadata item for the product in question is set to the new collection name
      and an audit message is logged that describes the change
    """) {
    new Context {
      when(dbFacade.collectionExists("abp_40_2001-02-03-04-05")) thenReturn true
      when(dbFacade.findMetadata(abp_40_ts12345)) thenReturn Some(CollectionMetadataItem(abp_40_ts12345, 10, None, Some(dateAgo(3600000))))

      val sc = new SwitchoverController(pta, status, workerFactory, dbFacade, auditClient,
        "db", scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("abp", 40, "2001-02-03-04-05"), request))

      assert(response.header.status / 100 === 2)
      worker.awaitCompletion()
      worker.terminate()

      verify(dbFacade).setCollectionInUseFor(abp_40_ts12345)
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
      //      when(db.collectionExists(anyString)) thenReturn false

      val sc = new SwitchoverController(pta, status, workerFactory, dbFacade, auditClient,
        "db", scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("abp", 40, "2001-02-03-04-05"), request))

      worker.awaitCompletion()
      worker.terminate()

      verify(dbFacade, never).setCollectionInUseFor(abp_40_ts12345)
      assert(logger.warns.size === 2, logger.all)
      assert(logger.warns.head.message === "Warn:abp_40_2001-02-03-04-05: collection was not found")
    }
  }

  test(
    """
      given that the intended collection exists but does not contain the metadata completedAt
      when valid parameters are passed to ingest
      then the switch-over fails due to conflict
      and the stored metadata item for the product in question is left unchanged
    """) {
    new Context {
      when(dbFacade.collectionExists("abp_40_2001-02-03-04-05")) thenReturn true
      when(dbFacade.findMetadata(abp_40_ts12345)) thenReturn Some(CollectionMetadataItem(abp_40_ts12345, 10, None, None))

      val sc = new SwitchoverController(pta, status, workerFactory, dbFacade, auditClient,
        "db", scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("abp", 40, "2001-02-03-04-05"), request))

      worker.awaitCompletion()
      worker.terminate()

      assert(response.header.status === 202)
      verify(dbFacade, never).setCollectionInUseFor(abp_40_ts12345)
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
      val sc = new SwitchoverController(new FailAuthAction, status, workerFactory, dbFacade, auditClient,
        "db", scala.concurrent.ExecutionContext.Implicits.global)
      val response = await(call(sc.doSwitchTo("abp", 40, "2001-02-03-04-05"), request))

      worker.awaitCompletion()
      worker.terminate()

      assert(response.header.status === 401)
      verify(dbFacade, never).setCollectionInUseFor(abp_40_ts12345)
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
      val sc = new SwitchoverController(pta, status, workerFactory, dbFacade, auditClient,
        "db", scala.concurrent.ExecutionContext.Implicits.global)
      val model1 = new StateModel("abp", 40, Some("full"), Some("2001-02-03-04-05"), hasFailed = true)

      val model2 = sc.switchIfOK(model1)

      worker.awaitCompletion()

      assert(model2 === model1)
      verify(dbFacade, never).setCollectionInUseFor(abp_40_ts12345)
      assert(logger.size === 1, logger.all.mkString("\n"))
      assert(logger.infos.map(_.message) === List("Info:Switchover was skipped."))

      worker.terminate()
    }
  }

  private def dateAgo(ms: Long) = {
    val now = System.currentTimeMillis
    new Date(now - ms)
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
