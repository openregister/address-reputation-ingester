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

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.{DBCollection, MongoException}
import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}


class OutputDBWriter(bulkSize: Int,
                     cleardownOnError: Boolean,
                     collectionNameRoot: String,
                     mongoDbConnection: CasbahMongoConnection,
                     logger: SimpleLogger = new LoggerFacade(Logger.logger)) extends OutputWriter {

  private var documentCount = 0

  lazy private val collection: DBCollection = mongoDbConnection.getConfiguredDb.getCollection(collectionName)
  private var bulk = collection.initializeUnorderedBulkOperation
  private var errored: Boolean = false

  private def collectionName = {
    var collectionName = collectionNameRoot
    var iteration = 0
    while (mongoDbConnection.getConfiguredDb.collectionExists(collectionName)) {
      iteration += 1
      collectionName = s"${collectionNameRoot}_${iteration}"
    }
    logger.info(s"Writing to collection ${collectionName}")
    collectionName
  }

  override def output: (DbAddress) => Unit = (address: DbAddress) => {
    try {
      documentCount += 1

      //use below to insert documents one at a time (and comment out bulk processing\
      //collection.insert(MongoDBObject(address.tupled))

      bulk.insert(MongoDBObject(address.tupled))
      if (documentCount % bulkSize == 0) {
        logger.info("Committing bulk ")
        bulk.execute()
        bulk = collection.initializeUnorderedBulkOperation
      }
    } catch {
      case me: MongoException =>
        logger.info(s"Caught Mongo Exception processing bulk insertion ${me}")
        errored = true
        throw me
    }
    ()
  }

  override def close(): Unit = {
    try {
      if (documentCount % bulkSize != 0) bulk.execute()
      collection.createIndex(MongoDBObject("postcode" -> 1), MongoDBObject("unique" -> false))
    } catch {
      case me: MongoException =>
        logger.info(s"Caught MongoException committing final bulk insert and creating index ${me}")
        errored = true
    }

    if (errored) {
      logger.info("Error detected while loading data into mongo")
      if (cleardownOnError) collection.drop()
    } else {
      logger.info(s"number of documents loaded $documentCount")
    }
    mongoDbConnection.close()
    documentCount = 0
    errored = false
    ()
  }

}

class OutputDBWriterFactory extends OutputWriterFactory {

  private val mongoDbUri = mustGetConfigString(current.mode, current.configuration, "mongodb.uri")
  private val bulkSize = mustGetConfigInt(current.mode, current.configuration, "mongodb.bulkSize")
  private var cleardownOnError = mustGetConfigBoolean(current.mode, current.configuration, "mongodb.cleardownOnError")

  def writer(collectionNameRoot: String): OutputWriter = new OutputDBWriter(bulkSize, cleardownOnError, collectionNameRoot, new CasbahMongoConnection(mongoDbUri))
}




