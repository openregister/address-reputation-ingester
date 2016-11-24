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

package controllers.db

import java.util.Date

import controllers.{CollectionController, PassThroughAction}
import ingest.StubWorkerFactory
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.{CollectionMetadataItem, CollectionName}
import services.es.IndexMetadata
import services.exec.WorkQueue
import services.model.StatusLogger
import services.CollectionName
import uk.gov.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class CollectionControllerTest extends FunSuite with MockitoSugar {
// TODO: Re-enable and fix if necessary
//  class Context(otherCollections: Set[CollectionMetadataItem]) {
//    val logger = new StubLogger
//    val status = new StatusLogger(logger)
//    val worker = new WorkQueue(status)
//    val workerFactory = new StubWorkerFactory(worker)
//    val casbahMongoConnection = mock[CasbahMongoConnection]
//    val mongoDB = mock[MongoDB]
//    val indexMetadata = mock[IndexMetadata]
//
//    val adminCollections = Set(fakeIrrelevantCollection("admin"), fakeIrrelevantCollection("system.indexes"))
//    val collectionsInUse = Set(fakeCompletedCollection("abi_39_ts3"), fakeCompletedCollection("abp_40_ts5"))
//
//    val store = mock[MongoSystemMetadataStore]
//    val abi = new StoredMetadataStub("abi_39_ts3")
//    val abp = new StoredMetadataStub("abp_40_ts5")
//    when(store.addressBaseCollectionItem("abi")) thenReturn abi
//    when(store.addressBaseCollectionItem("abp")) thenReturn abp
//
//    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB
//
//    val all = adminCollections ++ collectionsInUse ++ otherCollections
//    val allNames = all.map(_.name.toString)
//    val collectionMap = allNames.map(n => n -> mock[MongoCollection]).toMap
//    when(mongoDB.collectionNames) thenReturn mutable.Set() ++ allNames
//
//    for (cmi <- all) {
//      val name = cmi.name.toString
//      val collection = collectionMap(name)
//      when(mongoDB.collectionExists(name)) thenReturn true
//      when(mongoDB(name)) thenReturn collection
//      when(collection.name) thenReturn name
//      when(collection.findOneByID("metadata")) thenReturn convert(cmi)
//    }
//
//    val collectionMetadata = new CollectionMetadata(mongoDB, store)
//    val collectionController = new CollectionController(new PassThroughAction, status, workerFactory, collectionMetadata)
//
//    def teardown() {
//      worker.terminate()
//    }
//  }
//
//  test(
//    """
//      given that there are irrelevant collections
//      and there are no previous collections for each product
//      then determineObsoleteCollections should choose none of them
//    """) {
//    new Context(Set(
//      fakeIrrelevantCollection("foo"),
//      fakeIrrelevantCollection("bar")
//    )) {
//      val chosen = collectionController.determineObsoleteCollections
//      assert(chosen === Set())
//    }
//  }
//
//  test(
//    """
//      given that there are some previous completed collections for each product
//      then determineObsoleteCollections should choose all but the last of them,
//      not counting the one currently in use
//    """) {
//    new Context(Set(
//      fakeCompletedCollection("abi_38_ts1"),
//      fakeCompletedCollection("abi_39_ts1"),
//      fakeCompletedCollection("abi_39_ts2"),
//      // collection in use: abi_39_ts3
//      fakeCompletedCollection("abp_39_ts7"),
//      fakeCompletedCollection("abp_40_ts1"),
//      fakeCompletedCollection("abp_40_ts2"),
//      fakeCompletedCollection("abp_40_ts3"),
//      fakeCompletedCollection("abp_40_ts4"),
//      // collection in use: abp_40_ts5
//      fakeIrrelevantCollection("bar")
//    )) {
//      val chosen = collectionController.determineObsoleteCollections
//      assert(chosen.map(_.name.toString) === Set(
//        "abi_38_ts1",
//        "abi_39_ts1",
//        "abp_39_ts7",
//        "abp_40_ts1",
//        "abp_40_ts2",
//        "abp_40_ts3"
//      ))
//    }
//  }
//
//  test(
//    """
//      given that there are some previous and some future completed collections for each product
//      then determineObsoleteCollections should choose all but the last of the previous ones
//      and leave all the future ones
//    """) {
//    new Context(Set(
//      fakeCompletedCollection("abi_38_ts1"),
//      fakeCompletedCollection("abi_39_ts1"),
//      fakeCompletedCollection("abi_39_ts2"),
//      // collection in use: abi_39_ts3
//      fakeCompletedCollection("abi_39_ts4"),
//      fakeCompletedCollection("abp_39_ts7"),
//      fakeCompletedCollection("abp_40_ts1"),
//      fakeCompletedCollection("abp_40_ts2"),
//      fakeCompletedCollection("abp_40_ts3"),
//      fakeCompletedCollection("abp_40_ts4"),
//      // collection in use: abp_40_ts5
//      fakeCompletedCollection("abp_41_ts1"),
//      fakeCompletedCollection("abp_41_ts2"),
//      fakeIrrelevantCollection("bar")
//    )) {
//      val chosen = collectionController.determineObsoleteCollections
//      assert(chosen.map(_.name.toString) === Set(
//        "abi_38_ts1",
//        "abi_39_ts1",
//        "abp_39_ts7",
//        "abp_40_ts1",
//        "abp_40_ts2",
//        "abp_40_ts3"
//      ))
//    }
//  }
//
//  test(
//    """
//      given that there are some previous and some future incomplete collections for each product
//      then determineObsoleteCollections should choose all of them
//    """) {
//    new Context(Set(
//      fakeIncompleteCollection("abi_38_ts1"),
//      fakeIncompleteCollection("abi_39_ts1"),
//      fakeIncompleteCollection("abi_39_ts2"),
//      // collection in use: abi_39_ts3
//      fakeIncompleteCollection("abi_39_ts4"),
//      fakeIncompleteCollection("abp_39_ts7"),
//      fakeIncompleteCollection("abp_40_ts1"),
//      fakeIncompleteCollection("abp_40_ts2"),
//      fakeIncompleteCollection("abp_40_ts3"),
//      fakeIncompleteCollection("abp_40_ts4"),
//      // collection in use: abp_40_ts5
//      fakeIncompleteCollection("abp_41_ts1"),
//      fakeIncompleteCollection("abp_41_ts2"),
//      fakeIrrelevantCollection("bar")
//    )) {
//      val chosen = collectionController.determineObsoleteCollections
//      assert(chosen.map(_.name.toString) === Set(
//        "abi_38_ts1",
//        "abi_39_ts1",
//        "abi_39_ts2",
//        "abi_39_ts4",
//        "abp_39_ts7",
//        "abp_40_ts1",
//        "abp_40_ts2",
//        "abp_40_ts3",
//        "abp_40_ts4",
//        "abp_41_ts1",
//        "abp_41_ts2"
//      ))
//    }
//  }
//
//  test(
//    """
//      given that there are a mixture of complete and incomplete collections for each product
//      then cleanup should remove the right ones only
//      and there should be a log message for each one
//    """) {
//    new Context(Set(
//      fakeIncompleteCollection("abi_38_ts1"),
//      fakeCompletedCollection("abi_39_ts1"),
//      fakeCompletedCollection("abi_39_ts2"),
//      // collection in use: abi_39_ts3
//      fakeIncompleteCollection("abi_39_ts4"),
//      fakeCompletedCollection("abi_39_ts5"),
//      fakeIrrelevantCollection("foo"),
//      fakeIrrelevantCollection("bar")
//    )) {
//      collectionController.cleanup()
//      assert(logger.all.size === 3, logger.all.mkString(","))
//      assert(logger.infos.map(_.message).toSet == Set(
//        "Deleting obsolete MongoDB collection abi_38_ts1",
//        "Deleting obsolete MongoDB collection abi_39_ts1",
//        "Deleting obsolete MongoDB collection abi_39_ts4"
//      ))
//      verify(collectionMap("abi_38_ts1")).drop()
//      verify(collectionMap("abi_39_ts1")).drop()
//      verify(collectionMap("abi_39_ts4")).drop()
//    }
//  }
//
//  val anyDate = Some(new Date(0))
//
//  private def fakeIrrelevantCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, Some(123), None, None)
//
//  private def fakeIncompleteCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, Some(123), anyDate, None)
//
//  private def fakeCompletedCollection(name: String) = CollectionMetadataItem(CollectionName(name).get, Some(123), anyDate, anyDate)
//
//  private def convert(cmi: CollectionMetadataItem) =
//    if (cmi.completedAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime, "completedAt" -> cmi.completedAt.get.getTime))
//    else if (cmi.createdAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime))
//    else None
}
