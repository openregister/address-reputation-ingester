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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class IndexControllerTest extends FunSuite with MockitoSugar {
  // TODO: Re-enable and fix if necessary
  //  class Context(otherIndexes: Set[IndexMetadataItem]) {
  //    val logger = new StubLogger
  //    val status = new StatusLogger(logger)
  //    val worker = new WorkQueue(status)
  //    val workerFactory = new StubWorkerFactory(worker)
  //    val casbahMongoConnection = mock[CasbahMongoConnection]
  //    val mongoDB = mock[MongoDB]
  //    val indexMetadata = mock[IndexMetadata]
  //
  //    val adminIndexes = Set(fakeIrrelevantIndex("admin"), fakeIrrelevantIndex("system.indexes"))
  //    val indexesInUse = Set(fakeCompletedIndex("abi_39_ts3"), fakeCompletedIndex("abp_40_ts5"))
  //
  //    val store = mock[MongoSystemMetadataStore]
  //    val abi = new StoredMetadataStub("abi_39_ts3")
  //    val abp = new StoredMetadataStub("abp_40_ts5")
  //    when(store.addressBaseIndexItem("abi")) thenReturn abi
  //    when(store.addressBaseIndexItem("abp")) thenReturn abp
  //
  //    when(casbahMongoConnection.getConfiguredDb) thenReturn mongoDB
  //
  //    val all = adminIndexes ++ indexesInUse ++ otherIndexes
  //    val allNames = all.map(_.name.toString)
  //    val indexMap = allNames.map(n => n -> mock[MongoIndex]).toMap
  //    when(mongoDB.indexNames) thenReturn mutable.Set() ++ allNames
  //
  //    for (cmi <- all) {
  //      val name = cmi.name.toString
  //      val index = indexMap(name)
  //      when(mongoDB.indexExists(name)) thenReturn true
  //      when(mongoDB(name)) thenReturn index
  //      when(index.name) thenReturn name
  //      when(index.findOneByID("metadata")) thenReturn convert(cmi)
  //    }
  //
  //    val indexMetadata = new IndexMetadata(mongoDB, store)
  //    val indexController = new IndexController(new PassThroughAction, status, workerFactory, indexMetadata)
  //
  //    def teardown() {
  //      worker.terminate()
  //    }
  //  }
  //
  //  test(
  //    """
  //      given that there are irrelevant indexes
  //      and there are no previous indexes for each product
  //      then determineObsoleteIndexes should choose none of them
  //    """) {
  //    new Context(Set(
  //      fakeIrrelevantIndex("foo"),
  //      fakeIrrelevantIndex("bar")
  //    )) {
  //      val chosen = indexController.determineObsoleteIndexes
  //      assert(chosen === Set())
  //    }
  //  }
  //
  //  test(
  //    """
  //      given that there are some previous completed indexes for each product
  //      then determineObsoleteIndexes should choose all but the last of them,
  //      not counting the one currently in use
  //    """) {
  //    new Context(Set(
  //      fakeCompletedIndex("abi_38_ts1"),
  //      fakeCompletedIndex("abi_39_ts1"),
  //      fakeCompletedIndex("abi_39_ts2"),
  //      // index in use: abi_39_ts3
  //      fakeCompletedIndex("abp_39_ts7"),
  //      fakeCompletedIndex("abp_40_ts1"),
  //      fakeCompletedIndex("abp_40_ts2"),
  //      fakeCompletedIndex("abp_40_ts3"),
  //      fakeCompletedIndex("abp_40_ts4"),
  //      // index in use: abp_40_ts5
  //      fakeIrrelevantIndex("bar")
  //    )) {
  //      val chosen = indexController.determineObsoleteIndexes
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
  //      given that there are some previous and some future completed indexes for each product
  //      then determineObsoleteIndexes should choose all but the last of the previous ones
  //      and leave all the future ones
  //    """) {
  //    new Context(Set(
  //      fakeCompletedIndex("abi_38_ts1"),
  //      fakeCompletedIndex("abi_39_ts1"),
  //      fakeCompletedIndex("abi_39_ts2"),
  //      // index in use: abi_39_ts3
  //      fakeCompletedIndex("abi_39_ts4"),
  //      fakeCompletedIndex("abp_39_ts7"),
  //      fakeCompletedIndex("abp_40_ts1"),
  //      fakeCompletedIndex("abp_40_ts2"),
  //      fakeCompletedIndex("abp_40_ts3"),
  //      fakeCompletedIndex("abp_40_ts4"),
  //      // index in use: abp_40_ts5
  //      fakeCompletedIndex("abp_41_ts1"),
  //      fakeCompletedIndex("abp_41_ts2"),
  //      fakeIrrelevantIndex("bar")
  //    )) {
  //      val chosen = indexController.determineObsoleteIndexes
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
  //      given that there are some previous and some future incomplete indexes for each product
  //      then determineObsoleteIndexes should choose all of them
  //    """) {
  //    new Context(Set(
  //      fakeIncompleteIndex("abi_38_ts1"),
  //      fakeIncompleteIndex("abi_39_ts1"),
  //      fakeIncompleteIndex("abi_39_ts2"),
  //      // index in use: abi_39_ts3
  //      fakeIncompleteIndex("abi_39_ts4"),
  //      fakeIncompleteIndex("abp_39_ts7"),
  //      fakeIncompleteIndex("abp_40_ts1"),
  //      fakeIncompleteIndex("abp_40_ts2"),
  //      fakeIncompleteIndex("abp_40_ts3"),
  //      fakeIncompleteIndex("abp_40_ts4"),
  //      // index in use: abp_40_ts5
  //      fakeIncompleteIndex("abp_41_ts1"),
  //      fakeIncompleteIndex("abp_41_ts2"),
  //      fakeIrrelevantIndex("bar")
  //    )) {
  //      val chosen = indexController.determineObsoleteIndexes
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
  //      given that there are a mixture of complete and incomplete indexes for each product
  //      then cleanup should remove the right ones only
  //      and there should be a log message for each one
  //    """) {
  //    new Context(Set(
  //      fakeIncompleteIndex("abi_38_ts1"),
  //      fakeCompletedIndex("abi_39_ts1"),
  //      fakeCompletedIndex("abi_39_ts2"),
  //      // index in use: abi_39_ts3
  //      fakeIncompleteIndex("abi_39_ts4"),
  //      fakeCompletedIndex("abi_39_ts5"),
  //      fakeIrrelevantIndex("foo"),
  //      fakeIrrelevantIndex("bar")
  //    )) {
  //      indexController.cleanup()
  //      assert(logger.all.size === 3, logger.all.mkString(","))
  //      assert(logger.infos.map(_.message).toSet == Set(
  //        "Deleting obsolete MongoDB index abi_38_ts1",
  //        "Deleting obsolete MongoDB index abi_39_ts1",
  //        "Deleting obsolete MongoDB index abi_39_ts4"
  //      ))
  //      verify(indexMap("abi_38_ts1")).drop()
  //      verify(indexMap("abi_39_ts1")).drop()
  //      verify(indexMap("abi_39_ts4")).drop()
  //    }
  //  }
  //
  //  val anyDate = Some(new Date(0))
  //
  //  private def fakeIrrelevantIndex(name: String) = IndexMetadataItem(IndexName(name).get, Some(123), None, None)
  //
  //  private def fakeIncompleteIndex(name: String) = IndexMetadataItem(IndexName(name).get, Some(123), anyDate, None)
  //
  //  private def fakeCompletedIndex(name: String) = IndexMetadataItem(IndexName(name).get, Some(123), anyDate, anyDate)
  //
  //  private def convert(cmi: IndexMetadataItem) =
  //    if (cmi.completedAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime, "completedAt" -> cmi.completedAt.get.getTime))
  //    else if (cmi.createdAt.isDefined) Some(MongoDBObject("createdAt" -> cmi.createdAt.get.getTime))
  //    else None
}
