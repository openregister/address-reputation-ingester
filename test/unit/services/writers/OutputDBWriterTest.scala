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

package services.writers

import java.util.Date

import com.mongodb.DBObject
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{BulkWriteOperation, MongoCollection, MongoDB}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class OutputDBWriterTest extends FunSuite {

  class Context(collections: String*) {
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val mongoDB = mock[MongoDB]
    val collection = mock[MongoCollection]
    val bulk = mock[BulkWriteOperation]
    val logger = new StubLogger()
    val model = new StateModel("x", 4)
    val status = new StatusLogger(logger)

    when(mongoDB(anyString())) thenReturn collection
    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB
    when(collection.initializeUnorderedBulkOperation) thenReturn bulk

    when(mongoDB.collectionNames()) thenReturn (mutable.Set() ++ collections)
    for (n <- collections) {
      when(mongoDB.collectionExists(n)) thenReturn true
    }
  }

  test(
    """
      when the model has no corresponding collection yet
      then targetExistsAndIsNewerThan will return None
    """) {
    new Context("admin", "x_1_001", "x_2_001", "x_3_001") {
      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      val result = outputDBWriter.existingTargetThatIsNewerThan(new Date())

      assert(result === None)
    }
  }


  test(
    """
      when the model has corresponding collections without any completion dates
      then targetExistsAndIsNewerThan will return None
    """) {
    new Context("admin", "x_4_001", "x_4_002") {
      when(collection.findOneByID("metadata")) thenReturn None

      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      val result = outputDBWriter.existingTargetThatIsNewerThan(new Date(System.currentTimeMillis - 86400000L))

      assert(result === None)
    }
  }


  test(
    """
      when the model has corresponding collections with old completion dates
      then targetExistsAndIsNewerThan will return None
    """) {
    new Context("admin", "x_4_001", "x_4_002") {
      val now = new Date()
      val yesterday = new Date(now.getTime - 86400000L)

      val metadata = MongoDBObject("completedAt" -> yesterday)
      when(collection.findOneByID("metadata")) thenReturn Some(metadata)

      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      val result = outputDBWriter.existingTargetThatIsNewerThan(now)

      assert(result === None)
    }
  }


  test(
    """
      when the model has corresponding collections with newish completion dates
      then targetExistsAndIsNewerThan will return the last collection name
    """) {
    new Context("admin", "x_4_001", "x_4_002") {
      val now = new Date()
      val yesterday = new Date(now.getTime - 86400000L)

      val metadata = MongoDBObject("completedAt" -> now)
      when(collection.findOneByID("metadata")) thenReturn Some(metadata)

      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      val result = outputDBWriter.existingTargetThatIsNewerThan(yesterday)

      assert(result === Some("x_4_002"))
    }
  }


  test(
    """
      when a DbAddress is passed to the writer
      then an insert is invoked
      and the collection name is chosen correctly
    """) {
    new Context("admin", "x_4_000", "x_4_001", "x_4_004") {
      val someDBAddress = DbAddress("id1", List("1 Foo Rue"), "Puddletown", "FX1 1XF")

      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      outputDBWriter.output(someDBAddress)

      assert(outputDBWriter.collectionName === "x_4_005")
      verify(bulk, times(1)).insert(any[DBObject])
    }
  }


  test(
    """
      when close is called on the writer
      then a completion timestamp document is written to the output collection
      and an index is created for the postcode field
      and then close is called on the mongoDB instance
    """) {
    new Context("admin", "x_4_000", "x_4_001", "x_4_004") {
      val outputDBWriter = new OutputDBWriter(false, model, status, casbahMongoConnection, WriterSettings(10, 0), logger)

      outputDBWriter.close(true)

      verify(collection).insert(any[DBObject])
      verify(collection).createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
    }
  }

}
