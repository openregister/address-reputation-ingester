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
 */

package services.elasticsearch

import com.sksamuel.elastic4s.ElasticClient
import config.ConfigHelper._
import org.elasticsearch.common.settings.Settings
import play.api.Play._

object ElasticsearchHelperConfig {
  private val esSettings: Settings = Settings.settingsBuilder().put("cluster.name", "address-reputation").build()

  lazy val getClients: List[ElasticClient] = {
    mustGetConfigStringList(current.mode, current.configuration, "elastic.uri").map { uri =>
      ElasticClient.transport(esSettings, uri)
    }
  }
}


object ElasticsearchHelper extends ElasticsearchHelper(
  ElasticsearchHelperConfig.getClients,
  getConfigBoolean(current.mode, current.configuration, "elastic.is-cluster").getOrElse(true)
)

class ElasticsearchHelper(val clients: List[ElasticClient], val isCluster: Boolean) {
  val replicaCount = "1"
  val ariAliasName = "address-reputation-data"
  val ariDocumentName = "address"
}
