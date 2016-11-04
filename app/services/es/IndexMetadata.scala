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
import ingest.writers.WriterSettings
import org.elasticsearch.common.unit.TimeValue
import services.DbFacade
import services.model.StatusLogger
import services.mongo.{CollectionMetadataItem, CollectionName}
import uk.gov.hmrc.address.services.es.ESAdminImpl

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

object IndexMetadata {
  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val indexAlias = "addressbase-index"
  val address = "address"
}

class IndexMetadata(xclients: List[ElasticClient], val isCluster: Boolean, numShards: Map[String, Int], status: StatusLogger, ec: ExecutionContext)
  extends ESAdminImpl(xclients, status, ec) with DbFacade {

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

  def existingCollectionNames: List[String] = existingIndexNames

  def collectionExists(name: String): Boolean = indexExists(name)

  def dropCollection(name: String) {
    deleteIndex(name)
  }

  def getCollectionInUseFor(product: String): Option[CollectionName] = {
    getIndexInUseFor(product).flatMap(n => CollectionName(n))
  }

  def numShards(productName: String): Int = {
    numShards.getOrElse(productName, 12)
  }

  def findMetadata(name: CollectionName): Option[CollectionMetadataItem] = {
    val index = name.toString
    val count = countDocuments(index, address)
    val settings = getIndexSettings(index)

    val completedDate = settings.get(completedAt).map(s => new Date(s.toLong))
    val bSize = settings.get(bulkSize)
    val lDelay = settings.get(loopDelay)
    val iDPA = settings.get(includeDPA)
    val iLPI = settings.get(includeLPI)
    val pref = settings.get(prefer)
    val sFilter = settings.get(streetFilter)

    Some(CollectionMetadataItem(name, count, None, completedDate, bSize, lDelay, iDPA, iLPI, pref, sFilter, aliasesFor(index)))
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    // not needed
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    writeIndexSettings(indexName, Map(completedAt -> date.getTime.toString))
  }

  def writeIngestSettingsTo(indexName: String, writerSettings: WriterSettings): Unit = {
    writeIndexSettings(indexName,
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

  def setCollectionInUse(collectionName: CollectionName) {
    val newIndexName = collectionName.toString
    val productName = collectionName.productName

    if (isCluster) {
      status.info(s"Increasing replication count for $newIndexName")
      setReplicationCount(newIndexName, 1)

      status.info(s"Waiting for $ariAliasName to go green after increasing replica count")
      waitForGreenStatus(newIndexName)
    }

    val priorIndexes = switchAliases(newIndexName, productName, ariAliasName)

    if (isCluster) {
      setReplicationCount(priorIndexes.head, 0)
    }
    waitForGreenStatus(newIndexName)
  }

  private def awaitAll[T](fr: Seq[Future[T]]): Seq[T] = {
    Await.result(Future.sequence(fr), Duration("60s"))
  }
}

