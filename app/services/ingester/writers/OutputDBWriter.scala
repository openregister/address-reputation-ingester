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

import java.util.Date

import com.mongodb._
import com.mongodb.casbah.commons.MongoDBObject
import config.ApplicationGlobal
import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.collection.JavaConverters._


class OutputDBWriterFactory extends OutputWriterFactory {

  private val cleardownOnError = mustGetConfigString(current.mode, current.configuration, "mongodb.cleardownOnError").toBoolean

  def writer(collectionNameRoot: String, settings: WriterSettings): OutputWriter =
    new OutputDBWriter(cleardownOnError, collectionNameRoot,
      ApplicationGlobal.mongoConnection,
      settings,
      new LoggerFacade(Logger.logger))
}


class OutputDBWriter(cleardownOnError: Boolean,
                     collectionNameRoot: String,
                     mongoDbConnection: CasbahMongoConnection,
                     settings: WriterSettings,
                     logger: SimpleLogger) extends OutputWriter {

  private val collectionName = {
    var iteration = 0
    var collectionName = s"${collectionNameRoot}_$iteration"
    while (mongoDbConnection.getConfiguredDb.collectionExists(collectionName)) {
      iteration += 1
      collectionName = s"${collectionNameRoot}_$iteration"
    }
    logger.info(s"Writing new collection '$collectionName'")
    collectionName
  }

  private val collection: DBCollection = mongoDbConnection.getConfiguredDb.getCollection(collectionName)
  private val bulk = new BatchedBulkOperation(settings, collection)

  private var count = 0
  private var errored = false

  override def output(address: DbAddress) {
    try {
      bulk.insert(address.id, MongoDBObject(address.tupled))
      count += 1
    } catch {
      case me: MongoException =>
        logger.info(s"Caught Mongo Exception processing bulk insertion $me")
        errored = true
        throw me
    }
  }

  override def close() {
    if (!errored) {
      try {
        completeTheCollection()
      } catch {
        case me: MongoException =>
          logger.info(s"Caught MongoException committing final bulk insert and creating index $me")
          errored = true
      }
    }

    if (errored) {
      logger.info("Error detected while loading data into MongoDB.")
      if (cleardownOnError) collection.drop()
    } else {
      logger.info(s"Loaded $count documents.")
    }
    errored = false
  }

  private def completeTheCollection() {
    bulk.close()

    collection.createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))

    val metadata = MongoDBObject("_id" -> "metadata", "completedAt" -> new Date())
    collection.insert(List(metadata).asJava)
  }
}


class BatchedBulkOperation(settings: WriterSettings, collection: DBCollection) {
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

