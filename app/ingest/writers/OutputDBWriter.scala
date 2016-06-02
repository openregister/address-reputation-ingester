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

import addressbase.Document
import com.mongodb._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import config.ApplicationGlobal
import ingest.WriterSettings
import services.model.StatusLogger
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection


class OutputDBWriterFactory {

  def writer(collectionName: String, indexedFields: Seq[String],
             statusLogger: StatusLogger, settings: WriterSettings): OutputDBWriter =
    new OutputDBWriter(collectionName, indexedFields, statusLogger,
      ApplicationGlobal.mongoConnection,
      settings)
}


class OutputDBWriter(collectionName: String,
                     indexedFields: Seq[String],
                     statusLogger: StatusLogger,
                     mongoDbConnection: CasbahMongoConnection,
                     settings: WriterSettings) {

  private val db = mongoDbConnection.getConfiguredDb

  private val collection = db(collectionName.toString)
  private val bulk = new BatchedBulkOperation(settings, collection)

  private var count = 0
  private var hasError = false

  def begin() {
    statusLogger.info(s"Writing new collection '$collectionName'")
    collection.drop()
    CollectionMetadata.writeCreationDateTo(collection)
  }

  def output(doc: Document) {
    output(MongoDBObject(doc.tupled))
  }

  def output(doc: DBObject) {
    try {
      count += 1
      bulk.insert(count.toString, doc)
    } catch {
      case me: MongoException =>
        statusLogger.warn(s"Caught MongoDB exception processing bulk insertion $me")
        hasError = true
        throw me
    }
  }

  def end(completed: Boolean): Boolean = {
    if (!hasError) {
      try {
        bulk.close()
        for (field <- indexedFields) {
          collection.createIndex(MongoDBObject(field -> 1), MongoDBObject("unique" -> false))
        }
        if (completed) {
          // we have finished! let's celebrate
          CollectionMetadata.writeCompletionDateTo(collection)
        }
      } catch {
        case me: MongoException =>
          statusLogger.warn(s"Caught MongoDB exception committing final bulk insert and creating index $me")
          hasError = true
      }
    }

    if (hasError) {
      statusLogger.info("Error detected while loading data into MongoDB.")
      collection.drop()
    } else {
      statusLogger.info(s"Loaded $count documents.")
    }

    hasError
  }

  def abort() {
    collection.drop()
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

