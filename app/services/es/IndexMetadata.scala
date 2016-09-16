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

import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.unit.TimeValue
import services.DbFacade
import services.db.{CollectionMetadataItem, CollectionName}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class IndexMetadata(esHelper: ElasticsearchHelper)(implicit val ec: ExecutionContext) extends DbFacade {

    private val address = esHelper.address
    private val metadata = "metadata"
    private val completedAt = "completedAt"
    private val mid = "mid"

  def collectionExists(name: String): Boolean = existingCollectionNames.contains(name)

  def dropCollection(name: String) {
    esHelper.clients foreach { client =>
      client.admin.indices.delete(new DeleteIndexRequest(name)).actionGet
    }
  }

  def existingCollectionNames: List[String] = {
    val client0 = esHelper.clients.head.java
    val healths = greenHealth()
    healths.getIndices.keySet.asScala.toList.sorted
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val index = name.toString
    greenHealth(index)

    val rMetadata = esHelper.clients.head.execute {
      get id mid from index / metadata
    }

    val rCount = esHelper.clients.head.execute {
      search in index / address size 0
    }

    val source = Option(rMetadata.await.source)
    val completedDate = source.map(s => new Date(s.asScala(completedAt).asInstanceOf[Long]))
    val count = rCount.await.totalHits

    Some(CollectionMetadataItem(name, count.toInt, None, completedDate))
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    // not needed
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    esHelper.clients foreach { client =>
      client execute {
        index into indexName -> metadata fields (
          completedAt -> date.getTime
          ) id mid
      }
    }
  }

  private def greenHealth(index: String*) = {
    val client0 = esHelper.clients.head.java
    client0.admin().cluster().prepareHealth(index:_*).setWaitForGreenStatus().setTimeout(twoSeconds).get
  }

  private val twoSeconds = TimeValue.timeValueSeconds(2)
}

object IndexMetadata {
}
