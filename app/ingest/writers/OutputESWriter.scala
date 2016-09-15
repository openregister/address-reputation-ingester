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

import com.sksamuel.elastic4s._
import com.sksamuel.elastic4s.mappings.FieldType.StringType
import services.es.{ElasticsearchHelper, Services}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class OutputESWriter(var model: StateModel, statusLogger: StatusLogger, esHelper: ElasticsearchHelper,
                     settings: WriterSettings) extends OutputWriter with ElasticDsl {

  val ariIndexName: String = model.collectionName.asIndexName

  override def existingTargetThatIsNewerThan(date: Date): Option[String] = {
    None
  }

  override def begin() {
    //connect to ES and prepare index
    esHelper.clients foreach { client =>
      client execute {
        create index ariIndexName shards 4 replicas 0 refreshInterval "60s" mappings {
          mapping(esHelper.ariDocumentName) fields(
            field("id") typed StringType,
            //TODO lines should be an array - perhaps?
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
      } await

      client execute {
        aliases(add alias esHelper.indexAlias on ariIndexName)
      } await
    }
  }

  override def output(a: DbAddress) {
    //Add document to batch
    addBulk(index into ariIndexName -> esHelper.ariDocumentName fields(
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
    esHelper.clients foreach { client =>
      if (bulkCount != 0) {
        val fr = client execute {
          bulk(
            bulkStatements
          )
        }

        Await.ready(fr, Duration.Inf) foreach { br =>
          if (br.hasFailures) {
            statusLogger.warn(s"Elasticserch failure processing bulk insertion - ${br.failureMessage}")
            model = model.copy(hasFailed = true)
            throw new Exception(s"Elasticserch failure processing bulk insertion - ${br.failureMessage}")
          }
        }
      }

      client execute {
        update settings ariIndexName set Map(
          "index.refresh_interval" -> "1s"
        )
      }
    }
    statusLogger.info(s"Finished ingesting to index $ariIndexName")
    model
  }

  private var bulkCount = 0
  private var bulkStatements = collection.mutable.Buffer[IndexDefinition]()

  private def addBulk(i: IndexDefinition) = {
    bulkStatements += i
    bulkCount += 1

    if (bulkCount >= settings.bulkSize) {
      val fr = esHelper.clients map { client =>
        client execute {
          bulk(
            bulkStatements
          )
        }
      }

      Await.result(Future.sequence(fr), Duration.Inf) foreach { br =>
        if (br.hasFailures) {
          statusLogger.warn(s"Elasticsearch failure processing bulk insertion - ${br.failureMessage}")
          model = model.copy(hasFailed = true)
          throw new Exception(s"Elasticsearch failure processing bulk insertion - ${br.failureMessage}")
        }
      }

      bulkCount = 0
      bulkStatements.clear()
      Thread.sleep(settings.loopDelay)
    }

  }
}

class OutputESWriterFactory extends OutputWriterFactory {
  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputESWriter = {
    new OutputESWriter(model, statusLogger, Services.elasticSearchService, settings)
  }
}
