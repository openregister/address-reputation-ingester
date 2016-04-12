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

import com.mongodb.DBCollection
import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.{LoggerFacade, SimpleLogger}


class OutputDBWriter(collectionNameRoot: String,
                     mongoDbConnection: CasbahMongoConnection,
                     logger: SimpleLogger = new LoggerFacade(Logger.logger)) extends OutputWriter {

  private var documentCount = 0

  private def collectionName = {
    var collectionName = collectionNameRoot
    var iteration = 0
    while (mongoDbConnection.getConfiguredDb.collectionExists(collectionName)) {
      iteration += 1
      collectionName = s"${collectionNameRoot}_${iteration}"
    }
    collectionName
  }

  lazy private val collection: DBCollection = mongoDbConnection.getConfiguredDb.getCollection(collectionName)

  override def output: (DbAddress) => Unit = (address: DbAddress) => {
    documentCount += 1
    collection.insert(address.mongoDBObject)
    ()
  }

  override def close(): Unit = {
    mongoDbConnection.close()
    logger.info(s"number of documents loaded $documentCount")
    documentCount = 0
    ()
  }

}

class OutputDBWriterFactory extends OutputWriterFactory {
  lazy private val mongoDbConnection = {
    val mongoDbUri = mustGetConfigString(current.mode, current.configuration, "mongodb.uri")

    Logger.warn(s"MongoDB: $mongoDbUri")
    new CasbahMongoConnection(mongoDbUri)
  }

  def writer(collectionNameRoot: String): OutputWriter = new OutputDBWriter(collectionNameRoot, mongoDbConnection)
}




