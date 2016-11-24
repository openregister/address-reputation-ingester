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
import config.Provenance
import ingest.writers.WriterSettings
import org.elasticsearch.common.unit.TimeValue
import services.{CollectionMetadataItem, DbFacade}
import services.model.StatusLogger
import services.CollectionName
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

  private val iCompletedAt = "index.completedAt"
  private val iBulkSize = "index.bulkSize"
  private val iLoopDelay = "index.loopDelay"
  private val iIncludeDPA = "index.includeDPA"
  private val iIncludeLPI = "index.includeLPI"
  private val iPrefer = "index.prefer"
  private val iStreetFilter = "index.streetFilter"
  private val iBuildVersion = "index.buildVersion"
  private val iBuildNumber = "index.buildNumber"
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

    val completedDate = settings.get(iCompletedAt).map(s => new Date(s.toLong))
    val bSize = settings.get(iBulkSize)
    val lDelay = settings.get(iLoopDelay)
    val iDPA = settings.get(iIncludeDPA)
    val iLPI = settings.get(iIncludeLPI)
    val pref = settings.get(iPrefer)
    val sFilter = settings.get(iStreetFilter)
    val buildVersion = settings.get(iBuildVersion)
    val buildNumber = settings.get(iBuildNumber)

    Some(CollectionMetadataItem(name = name, size = count, createdAt = None, completedAt = completedDate,
      bulkSize = bSize, loopDelay = lDelay,
      includeDPA = iDPA, includeLPI = iLPI, prefer = pref, streetFilter = sFilter,
      buildVersion = buildVersion, buildNumber = buildNumber,
      aliases = aliasesFor(index)))
  }

  def writeCreationDateTo(indexName: String, date: Date = new Date()) {
    // not needed
  }

  def writeCompletionDateTo(indexName: String, date: Date = new Date()) {
    writeIndexSettings(indexName, Map(iCompletedAt -> date.getTime.toString))
  }

  def writeIngestSettingsTo(indexName: String, writerSettings: WriterSettings) {
    val buildVersion = Provenance.version.map(iBuildVersion -> _)
    val buildNumber = Provenance.buildNumber.map(iBuildNumber -> _)
    writeIndexSettings(indexName,
      Map(
        iBulkSize -> writerSettings.bulkSize.toString,
        iLoopDelay -> writerSettings.loopDelay.toString,
        iIncludeDPA -> writerSettings.algorithm.includeDPA.toString,
        iIncludeLPI -> writerSettings.algorithm.includeLPI.toString,
        iPrefer -> writerSettings.algorithm.prefer,
        iStreetFilter -> writerSettings.algorithm.streetFilter.toString
      ) ++ buildVersion ++ buildNumber
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

