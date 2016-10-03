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

import com.carrotsearch.hppc.ObjectLookupContainer
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.unit.TimeValue
import services.DbFacade
import services.mongo.{CollectionMetadataItem, CollectionName}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

class IndexMetadata(val clients: List[ElasticClient], val isCluster: Boolean, numShards: Map[String, Int])
                   (implicit val ec: ExecutionContext) extends DbFacade {

  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val indexAlias = "addressbase-index"
  val address = "address"
  val metadata = "metadata"

  private val completedAt = "index.completedAt"

  def numShards(productName: String): Int = {
    numShards.getOrElse(productName, 12)
  }

  def collectionExists(name: String): Boolean = existingCollectionNames.contains(name)

  def dropCollection(name: String) {
    clients foreach { client =>
      client.admin.indices.delete(new DeleteIndexRequest(name)).actionGet
    }
  }

  def existingCollectionNames: List[String] = {
    val healths = clients.head.execute {
      get cluster health
    } await

    healths.getIndices.keySet.asScala.toList.sorted
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val index = name.toString
    val rCount = clients.head.execute {
      search in index / address size 0
    }

    val indexSettings = clients.head.execute {
      get settings index
    } await

    val completedDate = Option(indexSettings.getSetting(index, completedAt)).map(s => new Date(s.toLong))
    val count = rCount.await.totalHits

    Some(CollectionMetadataItem(name, count.toInt, None, completedDate))
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    // not needed
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    clients foreach { client =>
      client execute {
        closeIndex(indexName)
      } await

      client execute {
        update settings indexName set Map(completedAt -> date.getTime.toString)
      } await

      client.execute {
        openIndex(indexName)
      } await
    }
  }

  def getCollectionInUseFor(product: String): Option[CollectionName] = {
    aliasOf(product).flatMap(n => CollectionName(n))
  }

  def setCollectionInUseFor(name: CollectionName) {
    val inUse = aliasOf(name.productName)
    if (inUse.isDefined) {
      clients foreach { client =>
        client execute {
          aliases(
            remove alias name.productName on inUse.get,
            add alias name.productName on name.toString
          )
        } await
      }
    } else {
      clients foreach { client =>
        client execute {
          aliases(add alias name.productName on name.toString)
        } await
      }
    }
  }

  private def aliasOf(name: String): Option[String] = {
    val gar = clients.head.execute {
      getAlias(name)
    } await

    val olc: ObjectLookupContainer[String] = gar.getAliases.keys

    if (olc.isEmpty)
      None
    else {
      val names = olc.toArray
      //      assert(names.length == 1, names)
      val n = names(0).toString
      Some(n)
    }
  }

  private def greenHealth(index: String*) = {
    val client0 = clients.head.java
    client0.admin().cluster().prepareHealth(index: _*).setWaitForGreenStatus().setTimeout(twoSeconds).get
  }

  private val twoSeconds = TimeValue.timeValueSeconds(2)
}

object IndexMetadata {
}
