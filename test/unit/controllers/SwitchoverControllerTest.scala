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

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.inject.ApplicationLifecycle
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.audit.AuditClient
import services.exec.WorkQueue
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.services.es.{IndexMetadata, IndexMetadataItem, IndexName}
import uk.gov.hmrc.logging.StubLogger


@RunWith(classOf[JUnitRunner])
class SwitchoverControllerTest extends FreeSpec with MockitoSugar {

  implicit val system = ActorSystem("test")

  implicit def mat: Materializer = ActorMaterializer()

  private val abp_40_ts12345 = IndexName("abp_40_200102030405").get

  "when an invalid product is passed to ingest" - {
    "then an exception is thrown" in {
      parameterTest("$%", 40, "200102030405")
    }
  }

  def parameterTest(product: String, epoch: Int, timestamp: String): Unit = {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val auditClient = mock[AuditClient]
    val indexMetadata = mock[IndexMetadata]
    val lifecycle = mock[ApplicationLifecycle]

    val worker = new WorkQueue(lifecycle, status)

    val request = FakeRequest()

    val switchoverController = new SwitchoverController(status, worker, indexMetadata, auditClient)

    intercept[IllegalArgumentException] {
      await(call(switchoverController.doSwitchTo(product, epoch, timestamp), request))
    }
  }

  class Context {
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    val lifecycle = mock[ApplicationLifecycle]
    val auditClient = mock[AuditClient]
    val indexMetadata = mock[IndexMetadata]

    val worker = new WorkQueue(lifecycle, status)

    val request = FakeRequest()
  }

  "given that the intended index exists and contains the containedAt metadata" - {
    "when valid parameters are passed to doSwitchTo" - {
      """
          then a successful response is returned
          and the stored metadata item for the product in question is set to the new index name
          and an audit message is logged that describes the change
      """ in {
        new Context {
          when(indexMetadata.indexExists(abp_40_ts12345)) thenReturn true
          when(indexMetadata.findMetadata(abp_40_ts12345)) thenReturn Some(IndexMetadataItem(abp_40_ts12345, Some(10), Some(dateAgo(3600000))))

          val sc = new SwitchoverController(status, worker, indexMetadata, auditClient)
          val response = await(call(sc.doSwitchTo("abp", 40, "200102030405"), request))

          assert(response.header.status / 100 === 2)
          worker.awaitCompletion()
          worker.terminate()

          verify(indexMetadata).setIndexInUse(abp_40_ts12345)
          verify(auditClient).succeeded(Map("product" -> "abp", "epoch" -> "40", "newIndex" -> "abp_40_200102030405"))
        }
      }
    }
  }

  "given that the intended index does not exist" - {
    "when valid parameters are passed to ingest" - {
      """
         then the switch-over fails due to missing the index
         and the stored metadata item for the product in question is left unchanged
      """ in {
        new Context {
          val sc = new SwitchoverController(status, worker, indexMetadata, auditClient)
          val response = await(call(sc.doSwitchTo("abp", 40, "200102030405"), request))

          worker.awaitCompletion()
          worker.terminate()

          verify(indexMetadata, never).setIndexInUse(abp_40_ts12345)
          assert(logger.warns.size === 2, logger.all)
          assert(logger.warns.head.message === "abp_40_200102030405: index was not found")
        }
      }
    }
  }

  "given that the intended index exists but does not contain the metadata completedAt" - {
    "when valid parameters are passed to ingest" - {
      """
         then the switch-over fails due to conflict
         and the stored metadata item for the product in question is left unchanged
      """ in {
        new
            Context {
          when(indexMetadata.indexExists(abp_40_ts12345)) thenReturn true
          when(indexMetadata.findMetadata(abp_40_ts12345)) thenReturn Some(IndexMetadataItem(abp_40_ts12345, Some(10), None, None))

          val sc = new SwitchoverController(status, worker, indexMetadata, auditClient)
          val response = await(call(sc.doSwitchTo("abp", 40, "200102030405"), request))

          worker.awaitCompletion()
          worker.terminate()

          assert(response.header.status === 202)
          verify(indexMetadata, never).setIndexInUse(abp_40_ts12345)
          assert(logger.warns.size === 2, logger.all)
          assert(logger.warns.head.message === "abp_40_200102030405: index is still being written")
        }
      }
    }
  }

  "given a StateModel that is in a failed state" - {
    "when the inner switchIfOK method is called" - {
      """
        then no task is performed
        and the state model stays in its current state
      """ in {
        new Context {
          val sc = new SwitchoverController(status, worker, indexMetadata, auditClient)
          val model1 = new StateModel("abp", Some(40), Some("full"), Some("200102030405"), hasFailed = true)

          val model2 = sc.switchIfOK(model1)

          worker.awaitCompletion()

          assert(model2 === model1)
          verify(indexMetadata, never).setIndexInUse(abp_40_ts12345)
          assert(logger.size === 1, logger.all.mkString("\n"))
          assert(logger.infos.map(_.message) === List("Switchover was skipped."))

          worker.terminate()
        }
      }
    }
  }

  private def dateAgo(ms: Long) = {
    val now = System.currentTimeMillis
    new Date(now - ms)
  }
}
