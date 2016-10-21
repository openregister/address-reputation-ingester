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

package it.helper

import com.sksamuel.elastic4s.{ElasticClient, RichSearchResponse}
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.common.unit.TimeValue
import services.es.IndexMetadata
import uk.gov.hmrc.address.osgb.{DbAddress, DbAddressOrderingByLine1}
import uk.gov.hmrc.address.services.es.ESSchema
import uk.gov.hmrc.address.uk.Postcode

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Sorting

trait ESHelper {

  def esClient: ElasticClient

  def createSchema(idx: String, clients: List[ElasticClient]) {
    clients foreach { client =>
      client execute {
        ESSchema.createIndexDefinition(idx, IndexMetadata.address,
          ESSchema.Settings(1, 0, "1s"))
      } await()
    }
  }

  def waitForIndex(idx: String, timeout: TimeValue = TimeValue.timeValueSeconds(2)) {
    esClient.java.admin.cluster.prepareHealth(idx).setWaitForGreenStatus().setTimeout(timeout).get
  }

  def findPostcode(idx: String, postcode: Postcode)(implicit ec: ExecutionContext): Future[List[DbAddress]] = {
    val searchResponse = esClient.execute {
      search in idx -> IndexMetadata.address query matchQuery("postcode.raw", postcode.toString) routing postcode.toString size 100
    }
    searchResponse map convertSearchResponse
  }

  private def convertSearchResponse(response: RichSearchResponse): List[DbAddress] = {
    val arr: Array[DbAddress] = response.hits.map(hit => DbAddress.apply(hit.sourceAsMap))
    Sorting.quickSort(arr)(DbAddressOrderingByLine1)
    arr.toList
  }
}
