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

package services.writers

import java.util.Date

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.MongoDBObject
import services.model.StateModel


class CollectionMetadata(db: MongoDB, inputModel: StateModel) {

  lazy val existingCollectionNames: List[String] = {
    val collectionNamePrefix = inputModel.collectionBaseName + "_"
    db.collectionNames.filter(_.startsWith(collectionNamePrefix)).toList.sorted
  }

  private def indexOf(collectionName: String): Int = {
    val u = collectionName.lastIndexOf('_')
    collectionName.substring(u + 1).toInt
  }

  private lazy val nextFreeIndex =
    if (existingCollectionNames.isEmpty) 1
    else indexOf(existingCollectionNames.last) + 1

  def nextFreeCollectionName: String = CollectionMetadata.formatName(inputModel.collectionBaseName, nextFreeIndex)

  def revisedModel: StateModel = inputModel.copy(index = Some(nextFreeIndex))
}


object CollectionMetadata {
  private val metadata = "metadata"
  private val createdAt = "createdAt"
  private val completedAt = "completedAt"

  def formatName(collectionNamePrefix: String, index: Int): String = {
    "%s_%03d".format(collectionNamePrefix, index)
  }

  def writeCreationDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(createdAt -> date.getTime), upsert = true)
  }

  def writeCompletionDateTo(collection: MongoCollection, date: Date = new Date()) {
    val filter = MongoDBObject("_id" -> metadata)
    collection.update(filter, $inc(completedAt -> date.getTime), upsert = true)
  }

  def findMetadata(collection: MongoCollection): CollectionMetadataItem = {
    val m = collection.findOneByID(metadata)
    if (m.isEmpty) CollectionMetadataItem()
    else {
      val created = Option(m.get.get(createdAt)).map(n => new Date(n.asInstanceOf[Long]))
      val completed = Option(m.get.get(completedAt)).map(n => new Date(n.asInstanceOf[Long]))
      CollectionMetadataItem(created, completed)
    }
  }
}


case class CollectionMetadataItem(createdAt: Option[Date] = None,
                                  completedAt: Option[Date] = None)
