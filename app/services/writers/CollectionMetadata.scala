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

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoCollection, MongoDB}
import services.model.StateModel


class CollectionMetadata(db: MongoDB, inputModel: StateModel) {
  private val collectionNamePrefix = inputModel.collectionBaseName + "_"

  lazy val existingCollectionNames: List[String] = {
    db.collectionNames.filter(_.startsWith(collectionNamePrefix)).toList.sorted
  }

  private def indexOf(collectionName: String): Int = {
    collectionName.substring(collectionName.length - 3).toInt
  }

  private lazy val nextFreeIndex =
    if (existingCollectionNames.isEmpty) 1
    else indexOf(existingCollectionNames.last) + 1

  def nextFreeCollectionName: String =
    String.format("%s%03d", collectionNamePrefix, java.lang.Integer.valueOf(nextFreeIndex))

  def revisedModel: StateModel = inputModel.copy(index = Some(nextFreeIndex))
}


object CollectionMetadata {

  def writeCompletionDateTo(collection: MongoCollection, date: Date = new Date()) {
    val metadata = MongoDBObject("_id" -> "metadata", "completedAt" -> date)
    collection.insert(metadata)
  }

  def findCompletionDateIn(collection: MongoCollection): Option[Date] = {
    val metadata = collection.findOneByID("metadata")
    if (metadata.isEmpty) None
    else Option(metadata.get.get("completedAt").asInstanceOf[Date])
  }
}
