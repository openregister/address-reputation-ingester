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
import com.sksamuel.elastic4s.{ElasticClient, GetAliasDefinition}
import ingest.writers.WriterSettings
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.unit.TimeValue
import services.DbFacade
import services.model.StatusLogger
import services.mongo.{CollectionMetadataItem, CollectionName}

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object IndexMetadata {
  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val indexAlias = "addressbase-index"
  val address = "address"
}

class IndexMetadata(val clients: List[ElasticClient], val isCluster: Boolean, numShards: Map[String, Int], status: StatusLogger, ec: ExecutionContext)
  extends DbFacade {

  import IndexMetadata._

  private implicit val xec = ec

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

  private val tenMinutes = TimeValue.timeValueMinutes(10)

  def updateIndexSettings(indexName: String, settings: Map[String, String]) {
    clients foreach { client =>
      greenHealth(client, tenMinutes, indexName)

      client execute {
        closeIndex(indexName)
      } await()

      client execute {
        update settings indexName set settings
      } await()

      client.execute {
        openIndex(indexName)
      } await()

      greenHealth(client, tenMinutes, indexName)
    }
  }

  def waitForGreenStatus(indices: String*) {
    clients.foreach(client => greenHealth(client, tenMinutes, indices: _*))
  }

  private def greenHealth(client: ElasticClient, timeout: TimeValue, index: String*) = {
    client.java.admin().cluster().prepareHealth(index: _*).setWaitForGreenStatus().setTimeout(timeout).get
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
    indexesAliasedBy(product).headOption.flatMap(n => CollectionName(n))
  }

  private def indexesAliasedBy(aliasName: String): List[String] = {
    val gar = queryAliases(clients.head) {
      getAlias(aliasName)
    }
    gar.keys.toList
  }

  def aliasesFor(indexName: String): List[String] = {
    val found = allAliases.find(_._1 == indexName)
    val optionalMatchingList = found.toList map (_._2)
    optionalMatchingList.flatten
  }

  def allAliases: Map[String, List[String]] = {
    queryAliases(clients.head) {
      getAlias("*")
    }
  }

  /**
    * Result is map keyed by index name containing lists of aliases. Example:
    * Map(
    * abi_44_201610251708 -> List(abi, address-reputation-data),
    * abp_44_201610182310 -> List(abp, address-reputation-data)
    * )
    */
  private def queryAliases(client: ElasticClient)(gad: GetAliasDefinition): Map[String, List[String]] = {
    val gar = client.execute(gad) await()
    val rawTree = gar.getAliases.asScala
    val converted = rawTree.map {
      kv =>
        val k = kv.key
        val v = kv.value.asScala.map(_.alias).toList
        k -> v
    }
    converted.toMap
  }

  def setCollectionInUse(collectionName: CollectionName) {
    val newIndexName = collectionName.toString
    val productName = collectionName.productName

    if (isCluster) {
      status.info(s"Increasing replication count for $newIndexName")
      increaseReplicationCount(newIndexName)

      status.info(s"Waiting for $ariAliasName to go green after increasing replica count")
      waitForGreenStatus(newIndexName)
    }

    val priorIndexes = switchAliasesTo(newIndexName, productName)

    if (isCluster) {
      dropReplicationFromOldIndexes(priorIndexes)
    }
  }

  private def increaseReplicationCount(newIndexName: String) {
    val fr = clients map {
      client => Future {
        status.info(s"Setting replica count to $replicaCount for $ariAliasName")
        client execute {
          update settings newIndexName set Map(
            "index.number_of_replicas" -> replicaCount
          )
        } await()
      }
    }
    awaitAll(fr)
  }

  private def switchAliasesTo(newIndexName: String, productName: String): Set[String] = {
    val fr = clients map {
      client => Future {
        val existingMap = queryAliases(client) {
          getAlias(productName).on("*")
        }

        val existingIndexes = existingMap.keys.toSeq

        val removeStatements = existingIndexes.flatMap {
          indexName =>
            status.info(s"Removing index $indexName from $ariAliasName and $productName aliases")
            Seq(remove alias ariAliasName on indexName, remove alias productName on indexName)
        }

        val addStatements = Seq(add alias ariAliasName on newIndexName, add alias productName on newIndexName)

        status.info(s"Adding index $newIndexName to $ariAliasName and $productName")

        client execute {
          aliases(removeStatements ++ addStatements)
        } await()

        existingIndexes
      }
    }
    awaitAll(fr).flatten.toSet // will typically yield a set of just one name
  }

  private def dropReplicationFromOldIndexes(priorIndexes: Set[String]) {
    val fr = clients map {
      client => Future {
        priorIndexes.foreach {
          indexName =>
            status.info(s"Reducing replica count for $indexName to 0")
            client execute {
              update settings indexName set Map(
                "index.number_of_replicas" -> "0"
              )
            } await()
        }
      }
    }
    awaitAll(fr)
  }

  private def awaitAll[T](fr: Seq[Future[T]]): Seq[T] = {
    Await.result(Future.sequence(fr), Duration("60s"))
  }
}

