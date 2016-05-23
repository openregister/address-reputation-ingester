/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package ingest.writers

import java.util.Date

import com.mongodb.casbah.Imports._


class CollectionMetadata(db: MongoDB) {

  import CollectionMetadata._

  def collectionExists(name: String): Boolean = db.collectionExists(name)

  def dropCollection(name: String) {
    db(name).dropCollection()
  }

  def existingCollectionNamesLike(name: CollectionName): List[String] = {
    val collectionNamePrefix = name.toPrefix + "_"
    db.collectionNames.filter(_.startsWith(collectionNamePrefix)).toList.sorted
  }

  private def indexOf(collectionName: String): Int = {
    val u = collectionName.lastIndexOf('_')
    collectionName.substring(u + 1).toInt
  }

  private def nextFreeIndex(name: CollectionName) =
    if (existingCollectionNamesLike(name).isEmpty) 1
    else indexOf(existingCollectionNamesLike(name).last) + 1

  def nextFreeCollectionNameLike(name: CollectionName): CollectionName =
    name.copy(index = Some(nextFreeIndex(name)))

  def existingCollections: List[CollectionName] = {
    db.collectionNames.toList.sorted.flatMap(name => CollectionName(name))
  }

  def existingCollectionMetadata: List[CollectionMetadataItem] = {
    existingCollections.flatMap(name => findMetadata(name))
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val collection = db(name.toString)
    val size = collection.size
    val m = collection.findOneByID(metadata)
    if (m.isEmpty)
      Some(CollectionMetadataItem(name, size))
    else {
      val created = Option(m.get.get(createdAt)).map(n => new Date(n.asInstanceOf[Long]))
      val completed = Option(m.get.get(completedAt)).map(n => new Date(n.asInstanceOf[Long]))
      Some(CollectionMetadataItem(name, size, created, completed))
    }
  }
}


object CollectionMetadata {
  private val metadata = "metadata"
  private val createdAt = "createdAt"
  private val completedAt = "completedAt"

  def writeCreationDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(createdAt -> date.getTime), upsert = true)
  }

  def writeCompletionDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(completedAt -> date.getTime), upsert = true)
  }
}


case class CollectionMetadataItem(name: CollectionName,
                                  size: Int,
                                  createdAt: Option[Date] = None,
                                  completedAt: Option[Date] = None) {

  def completedAfter(date: Date): Boolean = completedAt.isDefined && completedAt.get.after(date)

  def isIncomplete = createdAt.isDefined && completedAt.isEmpty

  def isComplete = createdAt.isDefined && completedAt.isDefined
}

