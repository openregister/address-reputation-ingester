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

import com.sksamuel.elastic4s.ElasticClient
import config.ConfigHelper._
import org.elasticsearch.common.settings.Settings
import play.api.Play._

object Services {

  lazy val elasticSearchService = {
    val clusterName = mustGetConfigString(current.mode, current.configuration, "elastic.clustername")
    val connectionString = mustGetConfigString(current.mode, current.configuration, "elastic.uri")
    val isCluster = getConfigBoolean(current.mode, current.configuration, "elastic.is-cluster").getOrElse(true)

    ElasticsearchHelper(clusterName, connectionString, isCluster)
  }
}


object ElasticsearchHelper {
  // allows construction without loading Play
  def apply(clusterName: String, connectionString: String, isCluster: Boolean): ElasticsearchHelper = {
    val esSettings = Settings.settingsBuilder().put("cluster.name", clusterName).build()

    val clients = connectionString.split("\\+").map { uri =>
      ElasticClient.transport(esSettings, uri)
    }.toList

    new ElasticsearchHelper(clients, isCluster)
  }
}


class ElasticsearchHelper(val clients: List[ElasticClient], val isCluster: Boolean) {
  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val indexAlias = "addressbase-index"
  val address = "address"
}
