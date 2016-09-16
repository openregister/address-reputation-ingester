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
import org.elasticsearch.common.settings.Settings

import scala.concurrent.ExecutionContext


object ElasticsearchHelper {
  // allows construction without loading Play
  def apply(clusterName: String, connectionString: String, isCluster: Boolean, ec: ExecutionContext): IndexMetadata = {
    val esSettings = Settings.settingsBuilder().put("cluster.name", clusterName).build()

    val clients = connectionString.split("\\+").map { uri =>
      ElasticClient.transport(esSettings, uri)
    }.toList

    implicit val iec = ec
    new IndexMetadata(clients, isCluster)
  }
}

