/*
 * Copyright 2017 HM Revenue & Customs
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
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import play.api.inject.ApplicationLifecycle
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.exec.WorkQueue
import services.model.StatusLogger
import uk.gov.hmrc.address.services.es.{IndexMetadata, IndexMetadataItem, IndexName}
import uk.gov.hmrc.logging.StubLogger


@RunWith(classOf[JUnitRunner])
class IndexControllerTest extends FunSuite with MockitoSugar {
  implicit val system = ActorSystem("test")

  implicit def mat: Materializer = ActorMaterializer()

  trait Context {
    protected val idxName = "010101"
    private val logger = new StubLogger
    private val status = new StatusLogger(logger)
    private val lifecycle = mock[ApplicationLifecycle]
    protected val worker = new WorkQueue(lifecycle, status)
    protected val request = FakeRequest()

    protected val indexMetadata: IndexMetadata = mock[IndexMetadata]
    when(indexMetadata.indexExists(any())) thenReturn true
    when(indexMetadata.toggleDoNotDelete(any())) thenCallRealMethod()

    protected val ic = new IndexController(status, worker, indexMetadata)
  }

  test(
    """
      when doNotDelete is called
      then an accepted response is returned
    """) {
    new Context {
      private val futureResponse = call(ic.doDoNotDelete(idxName), request)

      private val response = await(futureResponse)
      assert(response.header.status === 202)
      worker.terminate()
    }
  }

  test(
    """
      when doNotDelete is called
      for an index which doesn't exist
      then a not found response is returned
    """) {

    new Context {
      when(indexMetadata.indexExists(any())) thenReturn false

      private val futureResponse = call(ic.doDoNotDelete(idxName), request)

      private val response = await(futureResponse)
      assert(response.header.status === 404)
      worker.terminate()
    }
  }

  test(
    """
      when doCleanup is called
      then an accepted response is returned
    """) {
    new Context {
      private val futureResponse = call(ic.doCleanup(), request)

      private val response = await(futureResponse)
      assert(response.header.status === 202)
      worker.terminate()
    }
  }

  test(
    """
       when determineObsoleteIndexes is called
       then the result includes all incomplete indexes
       and all complete indexes where doNotDelete is unset
       but not the two most recent complete indexes for each product
       nor the two system indexes
    """) {
    new Context {
      // -- GIVEN --
      val d1 = new Date()
      // abi: to be retained
      val abi39a = IndexMetadataItem(name = IndexName("abi", Some(39), Some("ts1")), size = Some(1), completedAt = Some(d1), doNotDelete = true)
      val abi39b = IndexMetadataItem(name = IndexName("abi", Some(39), Some("ts2")), size = Some(1), completedAt = Some(d1), doNotDelete = true)
      val abi40a = IndexMetadataItem(name = IndexName("abi", Some(40), Some("ts1")), size = Some(1), completedAt = Some(d1), doNotDelete = true)
      // oldest: to be deleted
      val abp40a = IndexMetadataItem(name = IndexName("abp", Some(40), Some("ts1")), size = Some(1), completedAt = Some(d1))
      val abp40b = IndexMetadataItem(name = IndexName("abp", Some(40), Some("ts2")), size = Some(1), completedAt = Some(d1))
      // second-most recent: to be retained
      val abp40c = IndexMetadataItem(name = IndexName("abp", Some(40), Some("ts3")), size = Some(1), completedAt = Some(d1))
      // incomplete: to be deleted
      val abp40d = IndexMetadataItem(name = IndexName("abp", Some(40), Some("ts4")), size = Some(1), completedAt = None)
      // in use: to be retained
      val abp41a = IndexMetadataItem(name = IndexName("abp", Some(41), Some("ts1")), size = Some(1), completedAt = Some(d1), doNotDelete = true)
      // future: to be retained
      val abp41b = IndexMetadataItem(name = IndexName("abp", Some(41), Some("ts2")), size = Some(1), completedAt = Some(d1))
      // admin: to be retained
      val admin = IndexMetadataItem(name = IndexName("admin", None, None), size = None, completedAt = None)
      // system: to be retained
      val system = IndexMetadataItem(name = IndexName("system.indexes", None, None), size = None, completedAt = None)

      // here's the test data
      val items = List(abi39a, abi39b, abi40a, abp40a, abp40b, abp40c, abp40d, abp41a, abp41b, admin, system)

      // need to ensure it meets the behaviour given by ESAdmin and IndexMetadata, namely that the list contains
      // distinct items and they are sorted by index name.
      val names = items.map(_.name)
      val sorted = names.sorted
      assert(names === sorted, "Cross-check failed")
      assert(items.distinct.size === items.size, "Cross-check failed")

      val expectToDelete = Set(abp40a, abp40b, abp40d)
      val expectToRetain = Set(abi39a, abi39b, abi40a, abp40c, abp41a, abp41b, admin, system)
      assert(items.toSet === expectToDelete ++ expectToRetain, "Cross-check failed")

      when(indexMetadata.existingIndexMetadata) thenReturn items
      when(indexMetadata.getIndexNameInUseFor("abi")) thenReturn Some(abi40a.name)
      when(indexMetadata.getIndexNameInUseFor("abp")) thenReturn Some(abp41a.name)

      // -- WHEN --

      private val toDelete = ic.determineObsoleteIndexes

      // -- THEN --

      assert(toDelete === expectToDelete)

      worker.terminate()
    }
  }

}
