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

import com.mongodb._
import com.mongodb.casbah.MongoCollection
import com.mongodb.casbah.commons.MongoDBObject
import config.ApplicationGlobal
import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}


class OutputDBWriterFactory extends OutputWriterFactory {

  private def cleardownOnError = mustGetConfigString(current.mode, current.configuration, "mongodb.cleardownOnError").toBoolean

  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputWriter =
    new OutputDBWriter(cleardownOnError, model, statusLogger,
      ApplicationGlobal.mongoConnection,
      settings,
      new LoggerFacade(Logger.logger))
}


class OutputDBWriter(cleardownOnError: Boolean,
                     var model: StateModel,
                     statusLogger: StatusLogger,
                     mongoDbConnection: CasbahMongoConnection,
                     settings: WriterSettings,
                     logger: SimpleLogger) extends OutputWriter {

  private val db = mongoDbConnection.getConfiguredDb

  private val collectionMetadata = new CollectionMetadata(db, model)
  private val collectionName = collectionMetadata.nextFreeCollectionName
  private val collection = db(collectionName)
  private val bulk = new BatchedBulkOperation(settings, collection)

  private var count = 0
  private var hasError = false

  model = collectionMetadata.revisedModel
  statusLogger.info(s"Writing new collection '$collectionName'")

  override def output(address: DbAddress) {
    try {
      bulk.insert(address.id, MongoDBObject(address.tupled))
      count += 1
    } catch {
      case me: MongoException =>
        statusLogger.warn(s"Caught Mongo Exception processing bulk insertion $me")
        model = model.copy(hasFailed = true)
        hasError = true
        throw me
    }
  }

  override def close(): StateModel = {
    if (!hasError) {
      try {
        completeTheCollection()
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

  private def completeTheCollection() {
    bulk.close()

    collection.createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))

    // we have finished! let's celebrate
    CollectionMetadata.writeCompletionDateTo(collection)
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

