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

import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.mappings.DynamicMapping
import com.sksamuel.elastic4s.mappings.FieldType.{ShortType, StringType}

object ESSchema {

  // This is provided because IndexSettings has a more tricky API
  case class Settings(shards: Int = 1, replicas: Int = 0, refreshInterval: String = "60s") {
    def settings: collection.mutable.Map[String, Any] = {
      val is = new IndexSettings()
      is.shards = shards
      is.replicas = replicas
      is.refreshInterval = refreshInterval
      is.settings
    }
  }

  def createIndexDefinition(indexName: String,
                            addressDocType: String = "address",
                            metadataDocType: String = "metadata",
                            settings: Settings = Settings()): CreateIndexDefinition = {

    val definition = CreateIndexDefinition(indexName)

    definition._settings.settings ++= settings.settings

    definition mappings {
      mapping(addressDocType) routing true dynamic DynamicMapping.Strict fields(
        field("id") typed StringType index NotAnalyzed,
        field("line1") typed StringType fields (
          field("raw") typed StringType index NotAnalyzed
          ) copyTo ("lines"),
        field("line2") typed StringType fields (
          field("raw") typed StringType index NotAnalyzed
          ) copyTo ("lines"),
        field("line3") typed StringType fields (
          field("raw") typed StringType index NotAnalyzed
          ) copyTo ("lines"),
        field("lines") typed StringType,
        field("town") typed StringType fields (
          field("raw") typed StringType index NotAnalyzed
          ),
        field("postcode") typed StringType fields (
          field("raw") typed StringType index NotAnalyzed
          ),
        field("subdivision") typed StringType index NotAnalyzed,
        field("country") typed StringType index NotAnalyzed,
        field("language") typed StringType index NotAnalyzed,
        field("blpuState") typed ShortType,
        field("logicalState") typed ShortType,
        field("localCustodianCode") typed ShortType,
        field("streetClass") typed ShortType
        )
    }
  }
}
