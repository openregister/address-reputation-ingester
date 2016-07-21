/*
 * Copyright 2016 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ingest.writers

import java.util.Date

import com.sksamuel.elastic4s.{ElasticClient, _}
import com.sksamuel.elastic4s.mappings.FieldType.StringType
import org.elasticsearch.common.settings.Settings
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress

class OutputESWriter(var model: StateModel, statusLogger: StatusLogger, client: ElasticClient) extends OutputWriter with ElasticDsl {

  val ariIndexName = "address-reputation-data"
  val ariDocumentName = "address"

  override def existingTargetThatIsNewerThan(date: Date): Option[String] = {
    None
  }

  override def begin() {
    //connect to ES and prepare index
    client execute {
      delete index ariIndexName
    }
    client execute {
      create index ariIndexName shards 5 replicas 0 refreshInterval "-1" mappings {
        mapping(ariDocumentName) fields(
          field("id") typed StringType,
          field("line1") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("line2") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("line3") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("town") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed
            ),
          field("postcode") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed
            )
          )
      }
    }
  }

  override def output(a: DbAddress) {
    //Add document to batch
    addBulk(index into ariIndexName -> ariDocumentName fields(
      "id" -> a.id,
      "line1" -> a.line1,
      "line2" -> a.line2,
      "line3" -> a.line3,
      "town" -> a.town,
      "postcode" -> a.postcode
      ) id a.id
    )
  }

  override def end(completed: Boolean): StateModel = {
    //close index update refresh settings etc
    if (bulkCount != 0) {
      client execute {
        bulk (
          bulkStatements
        )
      }
    }
    client execute {
      update settings ariIndexName set Map("index.refresh_interval" -> "1s")
    }
    model
  }

  private var bulkCount = 0
  private var bulkStatements = collection.mutable.Buffer[IndexDefinition]()

  private def addBulk(i: IndexDefinition) = {
    bulkStatements += i
    if (bulkCount > 1000) {
      client execute {
        bulk (
          bulkStatements
        )
      }
      bulkStatements.clear()
      bulkCount = 0
    } else {
      bulkCount += 1
    }
  }
}

class OutputESWriterFactory extends OutputWriterFactory {
  val esSettings: Settings = Settings.settingsBuilder().put("cluster.name", "address-reputation").build();
  val uri = ElasticsearchClientUri("elasticsearch://localhost:9300")

  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings) = {
    new OutputESWriter(model, statusLogger, ElasticClient.transport(esSettings, uri))
  }
}
