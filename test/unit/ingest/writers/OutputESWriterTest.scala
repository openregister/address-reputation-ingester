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

package ingest.writers

import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{BulkWriteOperation, MongoCollection, MongoDB}
import com.mongodb.{DBObject, WriteConcern}
import com.sksamuel.elastic4s.ElasticClient
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.es.IndexMetadata
import services.model.{StateModel, StatusLogger}
import services.mongo.{CollectionMetadata, MongoSystemMetadataStore}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class OutputESWriterTest extends FreeSpec {

  val now = new Date()
  val yesterday = new Date(now.getTime - 86400000L)

  class Context(timestamp: String, collectionNames: Set[String]) {
//    val mongoDB = mock[MongoDB]
//    val store = mock[MongoSystemMetadataStore]
    val esClient = mock[ElasticClient]
    val indexMetadata = mock[IndexMetadata]
//    val bulk = mock[BulkWriteOperation]
    val logger = new StubLogger()
    val model = new StateModel(productName = "x", epoch = 4, timestamp = Some(timestamp))
    val status = new StatusLogger(logger)

//    when(collectionMetadata.db) thenReturn mongoDB
//    when(collectionMetadata.systemMetadata) thenReturn store
    when(indexMetadata.clients) thenReturn List(esClient)
    when(indexMetadata.existingCollectionNames) thenReturn collectionNames.toList.sorted

    val all = collectionNames + ("x_4_" + timestamp)
    val collections = all.map(n => n -> mock[MongoCollection]).toMap

    for (n <- all) {
      val collection = collections(n)
//      when(mongoDB.collectionExists(n)) thenReturn true
//      when(mongoDB(n)) thenReturn collection
      when(collection.name) thenReturn n
//      when(collection.initializeUnorderedBulkOperation) thenReturn bulk
    }
  }

  "targetExistsAndIsNewerThan" - {
    "when the model has no corresponding collection yet" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts1", Set("admin", "x_1_ts1", "x_2_ts1", "x_3_ts1")) {
          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0))

          val result = outputESWriter.existingTargetThatIsNewerThan(new Date())

          assert(result === None)
        }
      }
    }


//    "when the model has corresponding collections without any completion dates" - {
//      "then targetExistsAndIsNewerThan will return None" in {
//        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
//          when(collections("x_4_ts1").findOneByID("metadata")) thenReturn None
//          when(collections("x_4_ts2").findOneByID("metadata")) thenReturn None
//
//          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0))
//
//          val result = outputESWriter.existingTargetThatIsNewerThan(yesterday)
//
//          assert(result === None)
//        }
//      }
//    }
//
//
//    "when the model has corresponding collections with old completion dates" - {
//      "then targetExistsAndIsNewerThan will return None" in {
//        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
//          val metadata = MongoDBObject("completedAt" -> yesterday.getTime)
//          when(collections("x_4_ts1").findOneByID("metadata")) thenReturn Some(metadata)
//          when(collections("x_4_ts2").findOneByID("metadata")) thenReturn Some(metadata)
//
//          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0))
//
//          val result = outputESWriter.existingTargetThatIsNewerThan(now)
//
//          assert(result === None)
//        }
//      }
//    }
//
//
//    "when the model has corresponding collections with newish completion dates" - {
//      "then targetExistsAndIsNewerThan will return the last collection name" in {
//        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
//          val now = new Date()
//          val yesterday = new Date(now.getTime - 86400000L)
//
//          val metadata = MongoDBObject("completedAt" -> now.getTime)
//          when(collections("x_4_ts1").findOneByID("metadata")) thenReturn Some(metadata)
//          when(collections("x_4_ts2").findOneByID("metadata")) thenReturn Some(metadata)
//
//          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0))
//          val result = outputESWriter.existingTargetThatIsNewerThan(yesterday)
//
//          assert(result === Some("x_4_ts2"))
//        }
//      }
//    }

//    "output" - {
//      "when a DbAddress is passed to the writer" - {
//        """
//         then an insert is invoked
//         and the collection name is chosen correctly
//        """ in {
//          new Context("ts5", Set("admin", "x_4_ts0", "x_4_ts1", "x_4_ts4")) {
//            val someDBAddress = DbAddress("id1", List("1 Foo Rue"), Some("Puddletown"), "FX1 1XF", Some("GB-ENG"))
//
//            val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0))
//
//            outputESWriter.output(someDBAddress)
//
//            assert(outputESWriter.collectionName.toString === "x_4_ts5")
//            verify(bulk, times(1)).insert(any[DBObject])
//          }
//        }
//      }
//    }
//
//    "end" - {
//      "when close is called on the writer" - {
//        """
//         then a completion timestamp document is written to the output collection
//         and an index is created for the postcode field
//         and then close is called on the mongoDB instance
//        """ in {
//          new Context("ts5", Set("admin", "x_4_ts0", "x_4_ts1", "x_4_ts4")) {
//            val outputDBWriter = new OutputESWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0))
//
//            outputDBWriter.end(true)
//
//            verify(collections("x_4_ts5")).update(any[DBObject], any[DBObject], any[Boolean], any[Boolean], any[WriteConcern], any[Option[Boolean]])
//            verify(collections("x_4_ts5")).createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
//          }
//        }
//      }
//    }
  }
}
