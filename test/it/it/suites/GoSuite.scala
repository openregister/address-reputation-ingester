/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package it.suites

import com.sksamuel.elastic4s.ElasticDsl._
import it.helper.ElasticsearchTestHelper._
import it.helper.{AppServerTestApi, Synopsis}
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.unit.TimeValue
import org.scalatest.{DoNotDiscover, FreeSpec, MustMatchers}
import play.api.Application
import play.api.test.Helpers._
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es._
import uk.gov.hmrc.address.uk.Postcode
import uk.gov.hmrc.logging.StubLogger

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent.{Await, Future}

@DoNotDiscover
class GoSuite()(implicit val app: Application, implicit val appEndpoint: String)
  extends FreeSpec with MustMatchers with AppServerTestApi  {

  private implicit val ec = play.api.libs.concurrent.Execution.Implicits.defaultContext

  val idle = Synopsis.OkText("idle")

  //-----------------------------------------------------------------------------------------------

  "es go resource happy journey" - {
    """
       * observe quiet status
       * start ingest
       * observe busy status
       * await successful outcome
       * observe quiet status
       * verify additional index metadata (loopDelay,bulkSize,includeDPA,includeLPI,prefer,streetFilter)
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("exeter" -> 1, "abi" -> 1, "abp" -> 1), statusLogger, ec)
      val start = System.currentTimeMillis()

      assert(waitUntil("/admin/status", idle) === idle)

      val request = newRequest("GET", "/go/via/file/to/es/exeter/1/sample?bulkSize=7&loopDelay=0&forceChange=true")
      val response = await(request.execute())
      response.status mustBe ACCEPTED

      val busy = Synopsis.OkText("busy automatically loading to es exeter/1/sample (forced)")
      assert(waitWhile("/admin/status", idle) === busy)
      assert(waitWhile("/admin/status", busy) === idle)

      val exeter1 = indexMetadata.existingIndexNamesLike(IndexName("exeter", Some(1))).last
      val health = waitForIndex(exeter1.formattedName, TimeValue.timeValueSeconds(30))
      assert(health.getStatus == ClusterHealthStatus.GREEN)

      // FIXME temporary fix: poll index metadata for around 10 seconds for document count; only fail after that time
      Thread.sleep(10000)
      val metadata = indexMetadata.findMetadata(exeter1).get
      metadata.size mustBe Some(48737) // one less than MongoDB because metadata stored in idx settings

      // (see similar tests in ExtractorTest)
      assert(metadata.bulkSize.get === "7")
      assert(metadata.loopDelay.get === "0")
      assert(metadata.includeDPA.get === "true")
      assert(metadata.includeLPI.get === "true")
      assert(metadata.streetFilter.get === "0")
      assert(metadata.prefer.get === "DPA")

      val ex46aw = await(findPostcode(exeter1, Postcode("EX4 6AW")))
      assert(ex46aw.size === 38)
      for (a <- ex46aw) {
        assert(a.postcode === "EX4 6AW")
        assert(a.town === Some("Exeter"))
      }
      assert(ex46aw.head.lines === List("33 Longbrook Street"))
    }
  }

  //-----------------------------------------------------------------------------------------------

  "es ingest resource - errors" - {
    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/ingest/from/file/to/es/abp/not-a-number/full").status === BAD_REQUEST)
      //TODO fix this
      //assert(get("/ingest/from/file/to/es/abp/1/not-a-number").status === BAD_REQUEST)
    }
  }


  //-----------------------------------------------------------------------------------------------

  "switch-over resource happy journey" - {
    """
       * attempt to switch to existing index that has completedAt metadata
       * should change the nominated index
    """ in {
      val idx = IndexName("abp", Some(39), Some("200102030405"))

      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)

      indexMetadata.clients foreach { client =>
        client execute {
          ESSchema.createIndexDefinition(idx.formattedName, IndexMetadata.address,
            ESSchema.Settings(1, 0, "1s"))
        } await()
      }

      indexMetadata.writeCompletionDateTo(idx)

      // TODO improve this
      Thread.sleep(100)

      val request = newRequest("GET", "/switch/es/abp/39/200102030405")
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp").get.formattedName
      assert(indexName === "abp_39_200102030405")
    }
  }

  "switch-over resource error journeys" - {
    """
       * attempt to switch to non-existent index
       * should not change the nominated index
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialIndexName = indexMetadata.getIndexNameInUseFor("abp")

      val request = newRequest("GET", "/switch/es/abp/39/209902030405")
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp")
      assert(indexName === initialIndexName)
    }

    """
       * attempt to switch to existing index that has no completedAt metadata
       * should not change the nominated index
    """ in {
      val statusLogger = new StatusLogger(new StubLogger(true))
      val esAdmin = new ESAdminImpl(List(esClient), statusLogger, ec, settings)
      val indexMetadata = new IndexMetadata(esAdmin, false, Map("abi" -> 1, "abp" -> 1), statusLogger, ec)
      val initialIndexName = indexMetadata.getIndexNameInUseFor("abp")

      val idx = IndexName("abp", Some(39), Some("209002030405"))
      createSchema(idx, indexMetadata.clients)
      waitForIndex(idx.formattedName)

      val request = newRequest("GET", "/switch/es/abp/39/209002030405")
      val response = await(request.execute())
      assert(response.status === ACCEPTED)
      assert(waitUntil("/admin/status", idle) === idle)

      val indexName = indexMetadata.getIndexNameInUseFor("abp")
      assert(indexName === initialIndexName)
    }

    """
       * passing bad parameters
       * should give 400
    """ in {
      assert(get("/switch/es/abp/not-a-number/1").status === BAD_REQUEST)
    }
  }

  private def await[T](future: Future[T], timeout: Duration = FiniteDuration(20, "s")): T = Await.result(future, timeout)
}
