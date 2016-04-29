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

package services.ingester.writers

import java.util

import com.mongodb._
import com.mongodb.casbah.MongoDB
import com.mongodb.casbah.commons.MongoDBObject
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.ingester.model.ABPModel
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class OutputDBWriterTest extends FunSuite {

  class Context {
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val mongoDB = mock[MongoDB]
    val collection = mock[DBCollection]
    val bulk = mock[BulkWriteOperation]
    val logger = new StubLogger()
    val model = new ABPModel("", 0, "", None, logger)

    when(mongoDB.collectionExists(anyString())) thenReturn false
    when(mongoDB.getCollection(anyString())) thenReturn collection
    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB
    when(collection.initializeUnorderedBulkOperation) thenReturn bulk
  }

  test(
    """
      when a DbAddress is passed to the writer
      then an insert is invoked
    """) {
    new Context {
      val someDBAddress = DbAddress("id1", List("1 Foo Rue"), "Puddletown", "FX1 1XF")

      val outputDBWriter = new OutputDBWriter(false, model, casbahMongoConnection, WriterSettings(10, 0), logger)

      outputDBWriter.output(someDBAddress)

      //verify(collection, times(1)).insert(any[DBObject])
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
    new Context {
      val outputDBWriter = new OutputDBWriter(false, model, casbahMongoConnection, WriterSettings(10, 0), logger)

      outputDBWriter.close()

      verify(collection).insert(any[util.List[DBObject]])
      verify(collection).createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
    }
  }

}
