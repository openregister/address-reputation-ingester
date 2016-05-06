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

import java.util.Date

import com.mongodb._
import com.mongodb.casbah.commons.MongoDBObject
import config.ApplicationGlobal
import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import services.model.StateModel
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}

import scala.annotation.tailrec
import scala.collection.JavaConverters._


class OutputDBWriterFactory extends OutputWriterFactory {

  private val cleardownOnError = mustGetConfigString(current.mode, current.configuration, "mongodb.cleardownOnError").toBoolean

  def writer(model: StateModel, settings: WriterSettings): OutputWriter =
    new OutputDBWriter(cleardownOnError, model,
      ApplicationGlobal.mongoConnection,
      settings,
      new LoggerFacade(Logger.logger))
}


class OutputDBWriter(cleardownOnError: Boolean,
                     model: StateModel,
                     mongoDbConnection: CasbahMongoConnection,
                     settings: WriterSettings,
                     logger: SimpleLogger) extends OutputWriter {

  private val collectionNameRoot = model.collectionBaseName

  private val db = mongoDbConnection.getConfiguredDb

  private def collectionExists(i: Int) = {
    val collectionName = s"${collectionNameRoot}_$i"
    db.collectionExists(collectionName)
  }

  @tailrec
  private def nextFreeIndex(i: Int): Int = {
    if (!collectionExists(i)) i
    else nextFreeIndex(i + 1)
  }

  private val index = nextFreeIndex(0)
  private val collectionName = s"${collectionNameRoot}_$index"
  private val collection: DBCollection = db.getCollection(collectionName)
  private val bulk = new BatchedBulkOperation(settings, collection)

  private var count = 0
  private var errored = false

  model.index = Some(index)
  model.statusLogger.info(s"Writing new collection '$collectionName'")

  override def output(address: DbAddress) {
    try {
      bulk.insert(address.id, MongoDBObject(address.tupled))
      count += 1
    } catch {
      case me: MongoException =>
        model.fail(s"Caught Mongo Exception processing bulk insertion $me")
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
          model.fail(s"Caught MongoException committing final bulk insert and creating index $me")
          errored = true
      }
    }

    if (errored) {
      model.statusLogger.info("Error detected while loading data into MongoDB.")
      if (cleardownOnError) collection.drop()
    } else {
      model.statusLogger.info(s"Loaded $count documents.")
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

