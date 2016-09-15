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

package services.es

import java.util.Date

import services.db.{CollectionMetadataItem, CollectionName}

class IndexMetadata(esHelper: ElasticsearchHelper) {

  //  private val metadata = "metadata"
  //  private val createdAt = "createdAt"
  //  private val completedAt = "completedAt"

  def collectionExists(name: String): Boolean = false // TODO

  def dropCollection(name: String) {
    //TODO
  }

  def existingCollectionNamesLike(name: CollectionName): List[String] = Nil // TODO

  def existingCollections: List[CollectionName] = {
    Nil // TODO
  }

  def existingCollectionMetadata: List[CollectionMetadataItem] = {
    existingCollections.flatMap(name => findMetadata(name))
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    //TODO
    None
//    val collection = db(name.toString)
//    val size = collection.size
//    val m = collection.findOneByID(metadata)
//    if (m.isEmpty)
//      Some(CollectionMetadataItem(name, size))
//    else {
//      val created = Option(m.get.get(createdAt)).map(n => new Date(n.asInstanceOf[Long]))
//      val completed = Option(m.get.get(completedAt)).map(n => new Date(n.asInstanceOf[Long]))
//      Some(CollectionMetadataItem(name, size, created, completed))
//    }
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    //    val filter = MongoDBObject("_id" -> metadata)
    //    collection.update(filter, $inc(createdAt -> date.getTime), upsert = true)
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    //    val filter = MongoDBObject("_id" -> metadata)
    //    collection.update(filter, $inc(completedAt -> date.getTime), upsert = true)
  }

}

object IndexMetadata {
}
