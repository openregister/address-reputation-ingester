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

import java.util.concurrent.TimeUnit

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.elasticsearch.client.transport.{NoNodeAvailableException, TransportClient}
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.LocalTransportAddress
import uk.co.hmrc.logging.SimpleLogger

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration


object ElasticsearchHelper {
  // allows construction without loading Play
  def apply(clusterName: String, connectionString: String, isCluster: Boolean, numShards: Map[String, Int],
            ec: ExecutionContext, logger: SimpleLogger): IndexMetadata = {
    val esSettings = Settings.settingsBuilder().put("cluster.name", clusterName).build()

    val clients = connectionString.split("\\+").map { uri =>
      checkStatus(ElasticClient.transport(esSettings, uri), logger)
    }.toList

    implicit val iec = ec
    new IndexMetadata(clients, isCluster, numShards)
  }

  def apply(ec: ExecutionContext): IndexMetadata = {
    implicit val iec = ec

    val esSettings = Settings.settingsBuilder()
      .put("http.enabled", false)
      .put("node.local", true)

    val esClient = TransportClient.builder()
    esClient.settings(esSettings.build)

    val tc = esClient.build()
    tc.addTransportAddress(new LocalTransportAddress("1"))

    val clients = List(ElasticClient.fromClient(tc))

    new IndexMetadata(clients, false, Map())
  }

  def checkStatus(client: ElasticClient, logger: SimpleLogger): ElasticClient = {
    logger.info("Getting cluster health... ")
    var timeout = 0
    while (timeout < 60) {
      try {
        val chr = client.execute {
          get cluster health
        } await (Duration(1, TimeUnit.MINUTES))

        val status = chr.getStatus match {
          case ClusterHealthStatus.GREEN => "green"
          case ClusterHealthStatus.YELLOW => "yellow"
          case ClusterHealthStatus.RED => "red"
          case _ => "invalid"
        }
        logger.info(s"cluster ${chr.getClusterName} status is $status")
        return client
      } catch {
        case nne: NoNodeAvailableException => {
          logger.info("Node not found retrying")
          Thread.sleep(1000)
        }
      }
      timeout = timeout + 1
    }
    client
  }
}
