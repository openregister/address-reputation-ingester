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

package services.mongo

import java.util.Date

import com.mongodb.casbah.Imports._
import ingest.writers.WriterSettings
import services.DbFacade


class CollectionMetadata(val db: MongoDB, val systemMetadata: MongoSystemMetadataStore) extends DbFacade {

  import CollectionMetadata._

  def collectionExists(name: String): Boolean = db.collectionExists(name)

  def dropCollection(name: String) {
    db(name).drop()
  }

  def existingCollectionNames: List[String] = db.collectionNames.toList.sorted

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val collection = db(name.toString)
    val size = collection.size
    val m = collection.findOneByID(metadata)
    if (m.isEmpty)
      Some(CollectionMetadataItem(name, size))

    else {
      val created = Option(m.get.get(createdAt)).map(n => new Date(n.asInstanceOf[Long]))
      val completed = Option(m.get.get(completedAt)).map(n => new Date(n.asInstanceOf[Long]))
      val bSize = Option(m.get.get(bulkSize)).map(n => n.asInstanceOf[String])
      val lDelay = Option(m.get.get(loopDelay)).map(n => n.asInstanceOf[String])
      val iDPA = Option(m.get.get(includeDPA)).map(n => n.asInstanceOf[String])
      val iLPI = Option(m.get.get(includeLPI)).map(n => n.asInstanceOf[String])
      val pref = Option(m.get.get(prefer)).map(n => n.asInstanceOf[String])
      val sFilter = Option(m.get.get(streetFilter)).map(n => n.asInstanceOf[String])

      Some(CollectionMetadataItem(name, size, created, completed, bSize, lDelay, iDPA, iLPI, pref, sFilter))
    }
  }

  def getCollectionInUseFor(product: String): Option[CollectionName] =
    CollectionName(systemMetadata.addressBaseCollectionItem(product).get)

  def setCollectionInUse(name: CollectionName) {
    val addressBaseCollectionName = systemMetadata.addressBaseCollectionItem(name.productName)
    addressBaseCollectionName.set(name.toString)
  }
}


object CollectionMetadata {
  private val metadata = "metadata"
  private val createdAt = "createdAt"
  private val completedAt = "completedAt"
  private val bulkSize = "bulkSize"
  private val loopDelay = "loopDelay"
  private val includeDPA = "includeDPA"
  private val includeLPI = "includeLPI"
  private val prefer = "prefer"
  private val streetFilter = "streetFilter"


  def writeCreationDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(createdAt -> date.getTime), upsert = true)
  }

  def writeCompletionDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(completedAt -> date.getTime), upsert = true)
  }

  def writeIngestSettingsTo(collection: MongoCollection, writerSettings: WriterSettings) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(
      filter,
      MongoDBObject(
        bulkSize -> writerSettings.bulkSize.toString,
        loopDelay -> writerSettings.loopDelay.toString,
        includeDPA -> writerSettings.algorithm.includeDPA.toString,
        includeLPI -> writerSettings.algorithm.includeLPI.toString,
        prefer -> writerSettings.algorithm.prefer,
        streetFilter -> writerSettings.algorithm.streetFilter.toString),
      upsert = true)
  }
}


case class CollectionMetadataItem(name: CollectionName,
                                  size: Int,
                                  createdAt: Option[Date] = None,
                                  completedAt: Option[Date] = None,
                                  bulkSize: Option[String] = None,
                                  loopDelay: Option[String] = None,
                                  includeDPA: Option[String] = None,
                                  includeLPI: Option[String] = None,
                                  prefer: Option[String] = None,
                                  streetFilter: Option[String] = None,
                                  aliases: List[String] = Nil) {

  def completedAfter(date: Date): Boolean = completedAt.isDefined && completedAt.get.after(date)

  def isIncomplete = createdAt.isDefined && completedAt.isEmpty

  def isComplete = createdAt.isDefined && completedAt.isDefined
}

