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

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import ingest.writers.WriterSettings
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.common.unit.TimeValue
import services.DbFacade
import services.mongo.{CollectionMetadataItem, CollectionName}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

object IndexMetadata {
  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val indexAlias = "addressbase-index"
  val address = "address"
}

class IndexMetadata(val clients: List[ElasticClient], val isCluster: Boolean, numShards: Map[String, Int])
                   (implicit val ec: ExecutionContext) extends DbFacade {

  import IndexMetadata._

  private val completedAt = "index.completedAt"
  private val bulkSize = "index.bulkSize"
  private val loopDelay = "index.loopDelay"
  private val includeDPA = "index.includeDPA"
  private val includeLPI = "index.includeLPI"
  private val prefer = "index.prefer"
  private val streetFilter = "index.streetFilter"
  private val twoSeconds = TimeValue.timeValueSeconds(2)

  def numShards(productName: String): Int = {
    numShards.getOrElse(productName, 12)
  }

  def collectionExists(name: String): Boolean = existingCollectionNames.contains(name)

  def existingCollectionNames: List[String] = {
    val healths = clients.head.execute {
      get cluster health
    } await()

    healths.getIndices.keySet.asScala.toList.sorted
  }

  def dropCollection(name: String) {
    clients foreach { client =>
      client.admin.indices.delete(new DeleteIndexRequest(name)).actionGet
    }
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val index = name.toString
    val rCount = clients.head.execute {
      search in index / address size 0
    }

    val indexSettings = clients.head.execute {
      get settings index
    } await()

    val completedDate = Option(indexSettings.getSetting(index, completedAt)).map(s => new Date(s.toLong))
    val bSize = Option(indexSettings.getSetting(index, bulkSize))
    val lDelay = Option(indexSettings.getSetting(index, loopDelay))
    val iDPA = Option(indexSettings.getSetting(index, includeDPA))
    val iLPI = Option(indexSettings.getSetting(index, includeLPI))
    val pref = Option(indexSettings.getSetting(index, prefer))
    val sFilter = Option(indexSettings.getSetting(index, streetFilter))
    val count = rCount.await.totalHits

    Some(CollectionMetadataItem(name, count.toInt, None, completedDate, bSize, lDelay, iDPA, iLPI, pref, sFilter,
      aliasesFor(index)))
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    // not needed
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    updateIndexSettings(indexName, Map(completedAt -> date.getTime.toString))
  }

  def updateIndexSettings(indexName: String, settings: Map[String, String]) {
    clients foreach { client =>

      greenHealth(TimeValue.timeValueMinutes(10), indexName)
      client execute {
        closeIndex(indexName)
      } await()

      client execute {
        update settings indexName set settings
      } await()

      client.execute {
        openIndex(indexName)
      } await()
    }
  }

  private def greenHealth(timeout: TimeValue, index: String*) = {
    val client0 = clients.head.java
    client0.admin().cluster().prepareHealth(index: _*).setWaitForGreenStatus().setTimeout(timeout).get
  }

  def writeIngestSettingsTo(indexName: String, writerSettings: WriterSettings): Unit = {
    updateIndexSettings(indexName,
      Map(
        bulkSize -> writerSettings.bulkSize.toString,
        loopDelay -> writerSettings.loopDelay.toString,
        includeDPA -> writerSettings.algorithm.includeDPA.toString,
        includeLPI -> writerSettings.algorithm.includeLPI.toString,
        prefer -> writerSettings.algorithm.prefer,
        streetFilter -> writerSettings.algorithm.streetFilter.toString
      )
    )
  }

  def getCollectionInUseFor(product: String): Option[CollectionName] = {
    aliasesOf(product).headOption.flatMap(n => CollectionName(n))
  }

  private def aliasesOf(aliasName: String): List[String] = {
    val gar = clients.head.execute {
      getAlias(aliasName)
    } await()

    gar.getAliases.keys.toArray.map(_.toString).toList
  }

  private def aliasesFor(indexName: String): List[String] = {
    val found = allAliases.find(_._1 == indexName)
    val optionalMatchingList = found.toList map (_._2)
    optionalMatchingList.flatten
  }

  def allAliases: Map[String, List[String]] = {
    val gar = clients.head.execute {
      getAlias("*")
    } await()

    val allAliases = gar.getAliases.asScala
    val converted = allAliases.map {
      kv =>
        val k = kv.key
        val v = kv.value.asScala.map(_.alias).toList
        k -> v
    }
    converted.toMap
  }

  def setCollectionInUseFor(name: CollectionName) {
    val inUse = aliasesOf(name.productName)
    if (inUse.nonEmpty) {
      clients foreach { client =>
        client execute {
          aliases(
            remove alias name.productName on inUse.head,
            add alias name.productName on name.toString
          )
        } await()
      }
    } else {
      clients foreach { client =>
        client execute {
          aliases(add alias name.productName on name.toString)
        } await()
      }
    }
  }
}

