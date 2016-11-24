/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the LSCTicense is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package ingest.writers

import java.util.Date

import com.sksamuel.elastic4s.admin.ClusterHealthDefinition
import com.sksamuel.elastic4s._
import org.elasticsearch.action.ActionFuture
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.delete.{DeleteIndexRequest, DeleteIndexResponse}
import org.elasticsearch.client.{AdminClient, Client, IndicesAdminClient}
import org.elasticsearch.cluster.health.ClusterIndexHealth
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.es.IndexMetadata
import services.model.StatusLogger
import services.CollectionName
import uk.gov.hmrc.logging.StubLogger

import scala.collection.mutable
import scala.concurrent.Future

@RunWith(classOf[JUnitRunner])
class CollectionMetadataTest extends FunSuite with MockitoSugar {

  val anyDate = new Date(0)
  val foo_38_ts1 = CollectionName("foo_38_ts1").get
  val foo_39_ts1 = CollectionName("foo_39_ts1").get
  val foo_40_ts1 = CollectionName("foo_40_ts1").get
  val foo_40_ts2 = CollectionName("foo_40_ts2").get
  val foo_40_ts3 = CollectionName("foo_40_ts3").get
  val foo_41_ts1 = CollectionName("foo_41_ts1").get

  class Context {
    val client = mock[ElasticClient]
    val clients = List(client)
    val logger = new StubLogger()
    val status = new StatusLogger(logger)
    implicit val ec = scala.concurrent.ExecutionContext.Implicits.global
    val indexMetadata = new IndexMetadata(clients, false, Map.empty, status, ec)
  }

  test("collectionExists facades the es.collectionExists() method") {
    new Context {
      val clusterHealthResponse = mock[ClusterHealthResponse]

      val indices = new java.util.HashMap[String, ClusterIndexHealth]()
      indices.put("foo", null)

      when(clusterHealthResponse.getIndices) thenReturn indices

      implicit object ClusterHealthDefinitionExecutable
        extends Executable[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse] {
        override def apply(c: Client, t: ClusterHealthDefinition): Future[ClusterHealthResponse] = {
          injectFuture(c.admin.cluster.health(t.build, _))
        }
      }
      when(client.execute[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse](any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])) thenReturn Future(clusterHealthResponse)

      val e = indexMetadata.collectionExists("foo")

      assert(e === true)
      verify(client, times(1)).execute(any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])
    }
  }

  test("dropCollection facades the collection.drop() method") {
    new Context {
      val indexDeleteResponse = mock[DeleteIndexResponse]

      val adminClient = mock[AdminClient]
      when(client.admin) thenReturn adminClient

      val indicesAdminClient = mock[IndicesAdminClient]
      when(adminClient.indices) thenReturn indicesAdminClient

      val future = mock[ActionFuture[DeleteIndexResponse]]
      when(future.actionGet) thenReturn mock[DeleteIndexResponse]
      when(indicesAdminClient.delete(any[DeleteIndexRequest])) thenReturn future

      indexMetadata.dropCollection("foo")

      verify(indicesAdminClient, times(1)).delete(any[DeleteIndexRequest])
    }
  }

  test("existingCollectionNamesLike returns collection names starting with a specified prefix") {
    new Context {
      val clusterHealthResponse = mock[ClusterHealthResponse]

      val indices = new java.util.HashMap[String, ClusterIndexHealth]()
      indices.put("foo_38_ts1", null)
      indices.put("foo_39_ts1", null)
      indices.put("foo_40_ts1", null)
      indices.put("foo_40_ts2", null)
      indices.put("foo_40_ts3", null)

      when(clusterHealthResponse.getIndices) thenReturn indices

      implicit object ClusterHealthDefinitionExecutable
        extends Executable[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse] {
        override def apply(c: Client, t: ClusterHealthDefinition): Future[ClusterHealthResponse] = {
          injectFuture(c.admin.cluster.health(t.build, _))
        }
      }
      when(client.execute[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse](any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])) thenReturn Future(clusterHealthResponse)

      val names = indexMetadata.existingCollectionNamesLike(CollectionName("foo_40_ts1").get)

      assert(names === List(CollectionName("foo_40_ts1").get, CollectionName("foo_40_ts2").get, CollectionName("foo_40_ts3").get))

      verify(client, times(1)).execute(any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])
    }
  }

  test("existingCollections returns collection names") {
    new Context {
      val clusterHealthResponse = mock[ClusterHealthResponse]

      val indices = new java.util.HashMap[String, ClusterIndexHealth]()
      indices.put("foo_38_ts1", null)
      indices.put("foo_39_ts1", null)
      indices.put("foo_40_ts1", null)
      indices.put("foo_40_ts2", null)
      indices.put("foo_40_ts3", null)

      when(clusterHealthResponse.getIndices) thenReturn indices

      implicit object ClusterHealthDefinitionExecutable
        extends Executable[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse] {
        override def apply(c: Client, t: ClusterHealthDefinition): Future[ClusterHealthResponse] = {
          injectFuture(c.admin.cluster.health(t.build, _))
        }
      }
      when(client.execute[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse](any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])) thenReturn Future(clusterHealthResponse)

      val names = indexMetadata.existingCollections

      assert(names === List(foo_38_ts1, foo_39_ts1, foo_40_ts1, foo_40_ts2, foo_40_ts3))

      verify(client, times(1)).execute(any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])
    }
  }
// TODO: Reintroduce if/when ES DSL replaced
//  test("existingCollectionMetadata returns collection names") {
//    new Context {
//      //given
////      val collection38 = mock[MongoCollection]
////      val collection39 = mock[MongoCollection]
////      val collection40 = mock[MongoCollection]
////      when(mongoDB.collectionNames()) thenReturn mutable.Set() ++ Set("foo_38_ts1", "foo_39_ts1", "foo_40_ts1")
////
////      when(mongoDB("foo_38_ts1")) thenReturn collection38
////      when(mongoDB("foo_39_ts1")) thenReturn collection39
////      when(mongoDB("foo_40_ts1")) thenReturn collection40
////
////      when(collection38.name) thenReturn "foo_38_ts1"
////      when(collection39.name) thenReturn "foo_39_ts1"
////      when(collection40.name) thenReturn "abp_40_ts1"
////
////      when(collection38.size) thenReturn 138
////      when(collection39.size) thenReturn 139
////      when(collection40.size) thenReturn 140
////
////      when(collection38.findOneByID("metadata")) thenReturn Some(MongoDBObject("createdAt" -> 0L))
////      when(collection39.findOneByID("metadata")) thenReturn Some(MongoDBObject("completedAt" -> 0L))
////      when(collection40.findOneByID("metadata")) thenReturn None
////
////      //when
////      val names = indexMetadata.existingCollectionMetadata
////
////      //then
////      val cmi38 = CollectionMetadataItem(foo_38_ts1, Some(138), Some(anyDate), None)
////      val cmi39 = CollectionMetadataItem(foo_39_ts1, Some(139), None, Some(anyDate))
////      val cmi40 = CollectionMetadataItem(foo_40_ts1, Some(140), None, None)
////      assert(names === List(cmi38, cmi39, cmi40))
////    }
//
//      val adminClient = mock[AdminClient]
//      when(client.admin) thenReturn adminClient
//
//      val indicesAdminClient = mock[IndicesAdminClient]
//      when(adminClient.indices) thenReturn indicesAdminClient
//
//      val indicesStatsRequestBuilder = mock[IndicesStatsRequestBuilder]
//      when(indicesAdminClient.prepareStats(anyString())) thenReturn indicesStatsRequestBuilder
//
//      when(indicesStatsRequestBuilder.all) thenReturn indicesStatsRequestBuilder
//
//      val listenableActionFuture = mock[ListenableActionFuture[IndicesStatsResponse]]
//      when(indicesStatsRequestBuilder.execute()) thenReturn listenableActionFuture
//
//      val indicesResponse = mock[IndicesStatsResponse]
//      when(listenableActionFuture.actionGet()) thenReturn indicesResponse
//
//      implicit object SearchDefinitionExecutable
//        extends Executable[SearchDefinition, SearchResponse, SearchResponse] {
//        override def apply(c: Client, t: SearchDefinition): Future[SearchResponse] = {
//          injectFuture(c.search(t.build, _))
//        }
//      }
//
//      val searchResponse = mock[SearchResponse]
//      when(client.execute[SearchDefinition, SearchResponse, SearchResponse](any[SearchDefinition])(any[Executable[SearchDefinition,SearchResponse,SearchResponse]])) thenReturn Future(searchResponse)
//
//      val clusterHealthResponse = mock[ClusterHealthResponse]
//
//      val indices = new java.util.HashMap[String, ClusterIndexHealth]()
//      indices.put("foo_38_ts1", null)
//      indices.put("foo_39_ts1", null)
//      indices.put("foo_40_ts1", null)
//
//      when(clusterHealthResponse.getIndices) thenReturn indices
//
//      implicit object ClusterHealthDefinitionExecutable
//        extends Executable[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse] {
//        override def apply(c: Client, t: ClusterHealthDefinition): Future[ClusterHealthResponse] = {
//          injectFuture(c.admin.cluster.health(t.build, _))
//        }
//      }
//      when(client.execute[ClusterHealthDefinition, ClusterHealthResponse, ClusterHealthResponse](any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])) thenReturn Future(clusterHealthResponse)
//
//      //when
//      val names = indexMetadata.existingCollectionMetadata
//
//      //then
//      val cmi38 = CollectionMetadataItem(foo_38_ts1, Some(138), Some(anyDate), None)
//      val cmi39 = CollectionMetadataItem(foo_39_ts1, Some(139), None, Some(anyDate))
//      val cmi40 = CollectionMetadataItem(foo_40_ts1, Some(140), None, None)
//      assert(names === List(cmi38, cmi39, cmi40))
//
//
//      verify(client, times(1)).execute(any[ClusterHealthDefinition])(any[Executable[ClusterHealthDefinition,ClusterHealthResponse,ClusterHealthResponse]])
//    }
//
//  }

}
