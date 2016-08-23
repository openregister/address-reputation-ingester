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

import com.carrotsearch.hppc.ObjectLookupContainer
import com.sksamuel.elastic4s.ElasticDsl.add
import com.sksamuel.elastic4s.mappings.FieldType.StringType
import com.sksamuel.elastic4s.{ElasticClient, _}
import config.ConfigHelper._
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse
import org.elasticsearch.common.settings.Settings
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.Play._
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress

import scala.concurrent.Future
import scala.util.Try

class OutputESWriter(var model: StateModel, statusLogger: StatusLogger, client: ElasticClient, settings: WriterSettings) extends OutputWriter with ElasticDsl {

  import scala.concurrent.ExecutionContext.Implicits.global

  val ariAliasName = "address-reputation-data"
  val ariDocumentName = "address"
  val ariIndexName: String = model.collectionName.asIndexName

  override def existingTargetThatIsNewerThan(date: Date): Option[String] = {
    None
  }

  override def begin() {
    //connect to ES and prepare index
    statusLogger.info("1: indexName {}", ariIndexName)
    client execute {
      create index ariIndexName shards 5 replicas 0 refreshInterval "60s" mappings {
        mapping(ariDocumentName) fields(
          field("id") typed StringType,
          //TODO lines should be an array
          field("line1") typed StringType fields(
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("line2") typed StringType fields(
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("line3") typed StringType fields(
            field("raw") typed StringType index NotAnalyzed,
            field("lines") typed StringType
            ),
          field("town") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed
            ),
          field("postcode") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed
            ),
          field("subdivision") typed StringType fields (
            field("raw") typed StringType index NotAnalyzed
            )
          )
      }
    }
  }

  override def output(a: DbAddress) {
    //Add document to batch
    addBulk(index into ariIndexName -> ariDocumentName fields(
      //TODO should just use a.tupled
      "id" -> a.id,
      "line1" -> a.line1,
      "line2" -> a.line2,
      "line3" -> a.line3,
      "town" -> a.town.get,
      "postcode" -> a.postcode,
      "subdivision" -> a.subdivision.get
      ) id a.id
    )
  }

  override def end(completed: Boolean): StateModel = {
    //close index update refresh settings etc
    if (bulkCount != 0) {
      client execute {
        bulk(
          bulkStatements
        )
      }
    }
    rollAlias()
    statusLogger.info("1: indexName {}", ariIndexName)
    client execute {
      update settings ariIndexName set Map("index.refresh_interval" -> "1s")
    }
    model
  }

  private def rollAlias() = {
    val alias_resp: Future[GetAliasesResponse] = client execute {
      getAlias(model.collectionName.productName).on("*")
    }

    alias_resp.map(gar => {
      val olc = gar.getAliases().keys
      val aliasStatements: Array[MutateAliasDefinition] = olc.toArray().flatMap(a => {
        val alias = a.asInstanceOf[String]
        Array(remove alias ariAliasName on alias, remove alias model.collectionName.productName on alias)
      })

      val resp = client execute {
        aliases(
          aliasStatements ++
            Seq(
              add alias ariAliasName on ariIndexName,
              add alias model.collectionName.productName on ariIndexName
            )
      )}
      resp.isCompleted
    })

//      alias => {
//       Seq(remove alias ariAliasName on alias, remove alias model.collectionName.productName on alias)
//    })

  }

  private var bulkCount = 0
  private var bulkStatements = collection.mutable.Buffer[IndexDefinition]()

  private def addBulk(i: IndexDefinition) = {
    bulkStatements += i
    bulkCount += 1
    if (bulkCount >= settings.bulkSize) {
      bulkCount = 0
      val fr = client execute {
        bulk(
          bulkStatements
        )
      }
      fr map {
        r =>
          if (r.hasFailures) {
            statusLogger.warn(r.failureMessage)
          }
      }
      bulkStatements.clear()
      Thread.sleep(settings.loopDelay)
    }
  }
}

class OutputESWriterFactory extends OutputWriterFactory {
  val uri = ElasticsearchClientUri(mustGetConfigString(current.mode, current.configuration, "elastic.uri"))

  val esSettings: Settings = Settings.settingsBuilder().put("cluster.name", "address-reputation").build()

  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputESWriter = {
    new OutputESWriter(model, statusLogger, ElasticClient.transport(esSettings, uri), settings)
  }
}
