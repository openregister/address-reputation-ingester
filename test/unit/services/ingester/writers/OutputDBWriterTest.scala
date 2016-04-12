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

import com.mongodb.{DBCollection, DBObject}
import com.mongodb.casbah.MongoDB
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar.mock
import org.scalatest.FunSuite
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger

class OutputDBWriterTest extends FunSuite {

  test(
    """
      when a dbaddress is passed to the writer
      then an insert is invoked
    """) {
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val mongoDB = mock[MongoDB]
    val collection = mock[DBCollection]
    val someDBAddress = mock[DbAddress]
    val logger = new StubLogger()

    when(mongoDB.collectionExists(anyString())) thenReturn false
    when(mongoDB.getCollection(anyString())) thenReturn collection
    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB

    val outputDBWriter = new OutputDBWriter("", casbahMongoConnection, logger)

    outputDBWriter.output(someDBAddress)

    verify(collection, times(1)).insert(any[DBObject])
  }

  test(
    """
      when close is called on the writer
      then close is called on the mongoDB instance
    """) {
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val mongoDB = mock[MongoDB]
    val collection = mock[DBCollection]
    val someDBAddress = mock[DbAddress]
    val logger = new StubLogger()

    when(mongoDB.collectionExists(anyString())) thenReturn false
    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB

    val outputDBWriter = new OutputDBWriter("", casbahMongoConnection, logger)

    outputDBWriter.close()

    verify(casbahMongoConnection, times(1)).close()
  }

}
