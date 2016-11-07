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
 */

package ingest.writers

import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{BulkWriteOperation, MongoCollection, MongoDB}
import com.mongodb.{DBObject, WriteConcern}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.model.{StateModel, StatusLogger}
import services.mongo.{CollectionMetadata, CollectionMetadataItem, CollectionName, MongoSystemMetadataStore}
import uk.gov.hmrc.address.osgb.DbAddress
import uk.gov.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class OutputDBWriterTest extends FreeSpec {

  val now = new Date()
  val yesterday = new Date(now.getTime - 86400000L)

  val x_1_ts1 = CollectionName("x_1_ts1").get
  val x_4_ts1 = CollectionName("x_4_ts1").get
  val x_4_ts2 = CollectionName("x_4_ts2").get

  class Context(timestamp: String, collectionNames: Set[String]) {
    val mongoDB = mock[MongoDB]
    val store = mock[MongoSystemMetadataStore]
    val collectionMetadata = mock[CollectionMetadata]
    val bulk = mock[BulkWriteOperation]
    val logger = new StubLogger()
    val model = new StateModel(productName = "x", epoch = 4, timestamp = Some(timestamp))
    val status = new StatusLogger(logger)

    when(collectionMetadata.db) thenReturn mongoDB
    when(collectionMetadata.systemMetadata) thenReturn store
    when(collectionMetadata.existingCollectionNames) thenReturn collectionNames.toList.sorted

    val all = collectionNames + ("x_4_" + timestamp)
    val collections = all.map(n => n -> mock[MongoCollection]).toMap

    for (n <- all) {
      val collection = collections(n)
      when(mongoDB.collectionExists(n)) thenReturn true
      when(mongoDB(n)) thenReturn collection
      when(collection.name) thenReturn n
      when(collection.initializeUnorderedBulkOperation) thenReturn bulk
    }
  }

  "targetExistsAndIsNewerThan" - {
    "when the model has no corresponding collection yet" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts1", Set("admin", "x_1_ts1", "x_2_ts1", "x_3_ts1")) {
          val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

          val result = outputDBWriter.existingTargetThatIsNewerThan(new Date())

          assert(result === None)
        }
      }
    }


    "when the model has corresponding collections without any completion dates" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          when(collectionMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(collectionMetadata.findMetadata(x_4_ts1)) thenReturn None
          when(collectionMetadata.findMetadata(x_4_ts2)) thenReturn None
          val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

          val result = outputDBWriter.existingTargetThatIsNewerThan(yesterday)

          assert(result === None)
        }
      }
    }


    "when the model has corresponding collections with old completion dates" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          when(collectionMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(collectionMetadata.findMetadata(x_4_ts1)) thenReturn None
          when(collectionMetadata.findMetadata(x_4_ts2)) thenReturn None

          val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

          val result = outputDBWriter.existingTargetThatIsNewerThan(now)

          assert(result === None)
        }
      }
    }

    "when the model has corresponding collections with newish completion dates" - {
      "then targetExistsAndIsNewerThan will return the last collection name" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          val now = new Date()
          val yesterday = new Date(now.getTime - 86400000L)

          when(collectionMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(collectionMetadata.findMetadata(x_4_ts1)) thenReturn Some(CollectionMetadataItem(x_4_ts1, Some(10), None, Some(dateAgo(864000000))))
          when(collectionMetadata.findMetadata(x_4_ts2)) thenReturn Some(CollectionMetadataItem(x_4_ts2, Some(10), None, Some(dateAgo(1000))))

          val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

          val result = outputDBWriter.existingTargetThatIsNewerThan(yesterday)

          assert(result === Some("x_4_ts2"))
        }
      }
    }

    "output" - {
      "when a DbAddress is passed to the writer" - {
        """
         then an insert is invoked
         and the collection name is chosen correctly
        """ in {
          new Context("ts5", Set("admin", "x_4_ts0", "x_4_ts1", "x_4_ts4")) {
            val someDBAddress = DbAddress("id1", List("1 Foo Rue"), Some("Puddletown"), "FX1 1XF", Some("GB-ENG"),
              Some("UK"), Some(1234), Some("en"), None, None, None, None, None)

            val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

            outputDBWriter.output(someDBAddress)

            assert(outputDBWriter.collectionName.toString === "x_4_ts5")
            verify(bulk, times(1)).insert(any[DBObject])
          }
        }
      }
    }

    "end" - {
      "when close is called on the writer" - {
        """
         then a completion timestamp document is written to the output collection
         and an index is created for the postcode field
         and then close is called on the mongoDB instance
        """ in {
          new Context("ts5", Set("admin", "x_4_ts0", "x_4_ts1", "x_4_ts4")) {
            val outputDBWriter = new OutputDBWriter(false, model, status, collectionMetadata, WriterSettings(10, 0))

            outputDBWriter.end(true)

            verify(collections("x_4_ts5")).update(any[DBObject], any[DBObject], any[Boolean], any[Boolean], any[WriteConcern], any[Option[Boolean]])
            verify(collections("x_4_ts5")).createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
          }
        }
      }
    }
  }

  private def dateAgo(ms: Long) = {
    val now = System.currentTimeMillis
    new Date(now - ms)
  }
}
