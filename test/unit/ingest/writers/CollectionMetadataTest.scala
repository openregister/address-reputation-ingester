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

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoCollection, MongoDB}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.mongo.{CollectionMetadata, CollectionMetadataItem, CollectionName, MongoSystemMetadataStore}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection

import scala.collection.mutable

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
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val store = mock[MongoSystemMetadataStore]
    val mongoDB = mock[MongoDB]
    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB

    val collectionMetadata = new CollectionMetadata(mongoDB, store)
  }

  test("collectionExists facades the db.collectionExists() method") {
    new Context {
      when(mongoDB.collectionExists("foo")) thenReturn true

      val e = collectionMetadata.collectionExists("foo")

      assert(e === true)
      verify(mongoDB).collectionExists("foo")
    }
  }

  test("dropCollection facades the collection.drop() method") {
    new Context {
      val collection = mock[MongoCollection]
      when(mongoDB.apply("foo")) thenReturn collection

      collectionMetadata.dropCollection("foo")

      verify(mongoDB).apply("foo")
      verify(collection).drop()
    }
  }

  test("existingCollectionNamesLike returns collection names starting with a specified prefix") {
    new Context {
      val collection = mock[MongoCollection]
      when(mongoDB.collectionNames()) thenReturn mutable.Set() ++ Set("foo_38_ts1", "foo_39_ts1", "foo_40_ts1", "foo_40_ts2", "foo_40_ts3")

      val names = collectionMetadata.existingCollectionNamesLike(CollectionName("foo_40_ts1").get)

      assert(names === List(CollectionName("foo_40_ts1").get, CollectionName("foo_40_ts2").get, CollectionName("foo_40_ts3").get))
      verify(mongoDB).collectionNames()
    }
  }

  test("existingCollections returns collection names") {
    new Context {
      when(mongoDB.collectionNames()) thenReturn mutable.Set() ++ Set("foo_38_ts1", "foo_39_ts1", "foo_40_ts1", "foo_40_ts2", "foo_40_ts3")

      val names = collectionMetadata.existingCollections

      assert(names === List(foo_38_ts1, foo_39_ts1, foo_40_ts1, foo_40_ts2, foo_40_ts3))
      verify(mongoDB).collectionNames()
    }
  }

  test("existingCollectionMetadata returns collection names") {
    new Context {
      //given
      val collection38 = mock[MongoCollection]
      val collection39 = mock[MongoCollection]
      val collection40 = mock[MongoCollection]
      when(mongoDB.collectionNames()) thenReturn mutable.Set() ++ Set("foo_38_ts1", "foo_39_ts1", "foo_40_ts1")

      when(mongoDB("foo_38_ts1")) thenReturn collection38
      when(mongoDB("foo_39_ts1")) thenReturn collection39
      when(mongoDB("foo_40_ts1")) thenReturn collection40

      when(collection38.name) thenReturn "foo_38_ts1"
      when(collection39.name) thenReturn "foo_39_ts1"
      when(collection40.name) thenReturn "abp_40_ts1"

      when(collection38.size) thenReturn 138
      when(collection39.size) thenReturn 139
      when(collection40.size) thenReturn 140

      when(collection38.findOneByID("metadata")) thenReturn Some(MongoDBObject("createdAt" -> 0L))
      when(collection39.findOneByID("metadata")) thenReturn Some(MongoDBObject("completedAt" -> 0L))
      when(collection40.findOneByID("metadata")) thenReturn None

      //when
      val names = collectionMetadata.existingCollectionMetadata

      //then
      val cmi38 = CollectionMetadataItem(foo_38_ts1, 138, Some(anyDate), None)
      val cmi39 = CollectionMetadataItem(foo_39_ts1, 139, None, Some(anyDate))
      val cmi40 = CollectionMetadataItem(foo_40_ts1, 140, None, None)
      assert(names === List(cmi38, cmi39, cmi40))
    }
  }

}
