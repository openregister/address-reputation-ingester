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

package services

import services.mongo.{CollectionMetadataItem, CollectionName}

trait DbFacade {

  def collectionExists(name: String): Boolean

  def dropCollection(name: String)

  def existingCollectionNames: List[String] // sorted

  final def existingCollections: List[CollectionName] = {
    existingCollectionNames.flatMap(name => CollectionName(name))
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem]

  final def existingCollectionMetadata: List[CollectionMetadataItem] = {
    existingCollections.flatMap(name => findMetadata(name))
  }

  final def existingCollectionNamesLike(name: CollectionName): List[CollectionName] = {
    val collectionNamePrefix = name.toPrefix + "_"
    val stringNames = existingCollectionNames.filter(_.startsWith(collectionNamePrefix)).toList.sorted
    stringNames.flatMap(s => CollectionName.apply(s))
  }

  def getCollectionInUseFor(product: String): Option[CollectionName]

  def setCollectionInUse(name: CollectionName)
}
