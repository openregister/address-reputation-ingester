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

import com.mongodb._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import config.ApplicationGlobal
import config.ConfigHelper._
import play.api.Play._
import services.model.{StateModel, StatusLogger}
import services.mongo.CollectionMetadata
import uk.gov.hmrc.address.osgb.DbAddress

import scala.concurrent.ExecutionContext


class OutputDBWriter(cleardownOnError: Boolean,
                     var model: StateModel,
                     statusLogger: StatusLogger,
                     collectionMetadata: CollectionMetadata,
                     settings: WriterSettings) extends OutputWriter {

  val collectionName = model.collectionName
  private val collection = collectionMetadata.db(collectionName.toString)
  private val bulk = new BatchedBulkOperation(settings, collection)

  private var count = 0
  private var hasError = false

  def existingTargetThatIsNewerThan(date: Date): Option[String] = {
    val similar = collectionMetadata.existingCollectionNamesLike(collectionName)
    val found = similar.reverse.find {
      name =>
        val info = collectionMetadata.findMetadata(name)
        info.exists(_.completedAfter(date))
    }
    found.map(_.toString)
  }

  def begin() {
    statusLogger.info(s"Writing new collection '$collectionName'")
    CollectionMetadata.writeCreationDateTo(collection)
    CollectionMetadata.writeIngestSettingsTo(collection, settings)
  }

  def output(address: DbAddress) {
    try {
      val at = address.tupled ++ List("_id" -> address.id)
      bulk.insert(address.id, MongoDBObject(at))
      count += 1
    } catch {
      case me: MongoException =>
        statusLogger.warn(s"Caught Mongo Exception processing bulk insertion $me")
        model = model.copy(hasFailed = true)
        hasError = true
        throw me
    }
  }

  def end(completed: Boolean): StateModel = {
    if (!hasError) {
      try {
        bulk.close()
        collection.createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
        if (completed) {
          // we have finished! let's celebrate
          CollectionMetadata.writeCompletionDateTo(collection)
        }
      } catch {
        case me: MongoException =>
          statusLogger.warn(s"Caught MongoException committing final bulk insert and creating index $me")
          model = model.copy(hasFailed = true)
          hasError = true
      }
    }

    if (hasError) {
      statusLogger.info("Error detected while loading data into MongoDB.")
      if (cleardownOnError) collection.drop()
    } else {
      statusLogger.info(s"Loaded $count documents.")
    }
    hasError = false
    model
  }

}


class BatchedBulkOperation(settings: WriterSettings, collection: MongoCollection) {
  require(settings.bulkSize > 0)

  private var bulk = collection.initializeUnorderedBulkOperation
  private var count = 0

  private def reset() {
    bulk = collection.initializeUnorderedBulkOperation
    count = 0
  }

  def insert(id: String, document: DBObject) {
    //bulk.find(MongoDBObject("_id" -> id)).upsert().update(MongoDBObject("$setOnInsert" -> document))
    bulk.insert(document)
    count += 1

    if (count == settings.bulkSize) {
      bulk.execute()
      Thread.sleep(settings.loopDelay)
      reset()
    }
  }

  def close() {
    if (count > 0) bulk.execute()
    reset()
  }
}


class OutputDBWriterFactory extends OutputWriterFactory {

  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings, ec: ExecutionContext): OutputWriter =
    new OutputDBWriter(cleardownOnError, model, statusLogger, ApplicationGlobal.mongoCollectionMetadata, settings)

  private def cleardownOnError = mustGetConfigString(current.mode, current.configuration, "mongodb.cleardownOnError").toBoolean
}
