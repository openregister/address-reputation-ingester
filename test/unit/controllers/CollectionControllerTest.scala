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

package controllers

import java.util.Date

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoCollection, MongoDB}
import ingest.StubWorkerFactory
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.db.{CollectionMetadata, CollectionMetadataItem, CollectionName}
import services.elasticsearch.ElasticsearchHelper
import services.exec.WorkQueue
import services.model.StatusLogger
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class CollectionControllerTest extends FunSuite with MockitoSugar {

  class Context(otherCollections: Set[CollectionMetadataItem]) {
    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val worker = new WorkQueue(status)
    val workerFactory = new StubWorkerFactory(worker)
    val casbahMongoConnection = mock[CasbahMongoConnection]
    val mongoDB = mock[MongoDB]
    val helper = mock[ElasticsearchHelper]

    val adminCollections = Set(fakeIrrelevantCollection("admin"), fakeIrrelevantCollection("system.indexes"))
    val collectionsInUse = Set(fakeCompletedCollection("abi_39_003"), fakeCompletedCollection("abp_40_005"))

    val store = mock[SystemMetadataStore]
    val abi = new StoredMetadataStub("abi_39_003")
    val abp = new StoredMetadataStub("abp_40_005")
    when(store.addressBaseCollectionItem("abi")) thenReturn abi
    when(store.addressBaseCollectionItem("abp")) thenReturn abp

    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB

    val all = adminCollections ++ collectionsInUse ++ otherCollections
    val allNames = all.map(_.name.toString)
    val collectionMap = allNames.map(n => n -> mock[MongoCollection]).toMap
    when(mongoDB.collectionNames) thenReturn mutable.Set() ++ allNames

    for (cmi <- all) {
      val name = cmi.name.toString
      val collection = collectionMap(name)
      when(mongoDB.collectionExists(name)) thenReturn true
      when(mongoDB(name)) thenReturn collection
      when(collection.name) thenReturn name
      when(collection.findOneByID("metadata")) thenReturn convert(cmi)
    }

    val collectionMetadata = new CollectionMetadata(mongoDB)
    val collectionController = new CollectionController(new PassThroughAction, status, workerFactory, collectionMetadata, store, helper)

    def teardown() {
      worker.terminate()
    }
  }

  test(
    """
      given that there are irrelevant collections
      and there are no previous collections for each product
      then determineObsoleteCollections should choose none of them
    """) {
    new Context(Set(
      fakeIrrelevantCollection("foo"),
      fakeIrrelevantCollection("bar")
    )) {
      val chosen = collectionController.determineObsoleteCollections
      assert(chosen === Set())
    }
  }

  test(
    """
      given that there are some previous completed collections for each product
      then determineObsoleteCollections should choose all but the last of them,
      not counting the one currently in use
    """) {
    new Context(Set(
      fakeCompletedCollection("abi_38_001"),
      fakeCompletedCollection("abi_39_001"),
      fakeCompletedCollection("abi_39_002"),
      // collection in use: abi_39_003
      fakeCompletedCollection("abp_39_007"),
      fakeCompletedCollection("abp_40_001"),
      fakeCompletedCollection("abp_40_002"),
      fakeCompletedCollection("abp_40_003"),
      fakeCompletedCollection("abp_40_004"),
      // collection in use: abp_40_005
      fakeIrrelevantCollection("bar")
    )) {
      val chosen = collectionController.determineObsoleteCollections
      assert(chosen.map(_.name.toString) === Set(
        "abi_38_001",
        "abi_39_001",
        "abp_39_007",
        "abp_40_001",
        "abp_40_002",
        "abp_40_003"
      ))
    }
  }

  test(
    """
      given that there are some previous and some future completed collections for each product
      then determineObsoleteCollections should choose all but the last of the previous ones
      and leave all the future ones
    """) {
    new Context(Set(
      fakeCompletedCollection("abi_38_001"),
      fakeCompletedCollection("abi_39_001"),
      fakeCompletedCollection("abi_39_002"),
      // collection in use: abi_39_003
      fakeCompletedCollection("abi_39_004"),
      fakeCompletedCollection("abp_39_007"),
      fakeCompletedCollection("abp_40_001"),
      fakeCompletedCollection("abp_40_002"),
      fakeCompletedCollection("abp_40_003"),
      fakeCompletedCollection("abp_40_004"),
      // collection in use: abp_40_005
      fakeCompletedCollection("abp_41_001"),
      fakeCompletedCollection("abp_41_002"),
      fakeIrrelevantCollection("bar")
    )) {
      val chosen = collectionController.determineObsoleteCollections
      assert(chosen.map(_.name.toString) === Set(
        "abi_38_001",
        "abi_39_001",
        "abp_39_007",
        "abp_40_001",
        "abp_40_002",
        "abp_40_003"
      ))
    }
  }

  test(
    """
      given that there are some previous and some future incomplete collections for each product
      then determineObsoleteCollections should choose all of them
    """) {
    new Context(Set(
      fakeIncompleteCollection("abi_38_001"),
      fakeIncompleteCollection("abi_39_001"),
      fakeIncompleteCollection("abi_39_002"),
      // collection in use: abi_39_003
      fakeIncompleteCollection("abi_39_004"),
      fakeIncompleteCollection("abp_39_007"),
      fakeIncompleteCollection("abp_40_001"),
      fakeIncompleteCollection("abp_40_002"),
      fakeIncompleteCollection("abp_40_003"),
      fakeIncompleteCollection("abp_40_004"),
      // collection in use: abp_40_005
      fakeIncompleteCollection("abp_41_001"),
      fakeIncompleteCollection("abp_41_002"),
      fakeIrrelevantCollection("bar")
    )) {
      val chosen = collectionController.determineObsoleteCollections
      assert(chosen.map(_.name.toString) === Set(
        "abi_38_001",
        "abi_39_001",
        "abi_39_002",
        "abi_39_004",
        "abp_39_007",
        "abp_40_001",
        "abp_40_002",
        "abp_40_003",
        "abp_40_004",
        "abp_41_001",
        "abp_41_002"
      ))
    }
  }

  test(
    """
      given that there are a mixture of complete and incomplete collections for each product
      then cleanup should remove the right ones only
      and there should be a log message for each one
    """) {
    new Context(Set(
      fakeIncompleteCollection("abi_38_001"),
      fakeCompletedCollection("abi_39_001"),
      fakeCompletedCollection("abi_39_002"),
      // collection in use: abi_39_003
      fakeIncompleteCollection("abi_39_004"),
      fakeCompletedCollection("abi_39_005"),
      fakeIrrelevantCollection("foo"),
      fakeIrrelevantCollection("bar")
    )) {
      collectionController.cleanup()
      assert(logger.all.size === 3, logger.all.mkString(","))
      assert(logger.infos.map(_.message).toSet == Set(
        "Info:Deleting obsolete collection abi_38_001",
        "Info:Deleting obsolete collection abi_39_001",
        "Info:Deleting obsolete collection abi_39_004"
      ))
      verify(collectionMap("abi_38_001")).drop()
      verify(collectionMap("abi_39_001")).drop()
      verify(collectionMap("abi_39_004")).drop()
    }
  }

  val anyDate = Some(new Date(0))

  private def fakeIrrelevantCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, 123, None, None)

  private def fakeIncompleteCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, 123, anyDate, None)

  private def fakeCompletedCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, 123, anyDate, anyDate)

  private def convert(cmi: CollectionMetadataItem) =
    if (cmi.completedAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime, "completedAt" -> cmi.completedAt.get.getTime))
    else if (cmi.createdAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime))
    else None
}
