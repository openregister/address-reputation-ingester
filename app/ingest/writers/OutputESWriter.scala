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

import com.sksamuel.elastic4s.mappings.FieldType.StringType
import com.sksamuel.elastic4s.{ElasticClient, _}
import services.elasticsearch.ElasticsearchHelper
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress

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
          mapping(ElasticsearchHelper.ariDocumentName) fields(
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
      }
    }
  }

  override def output(a: DbAddress) {
    //Add document to batch
    addBulk(index into ariIndexName -> ElasticsearchHelper.ariDocumentName fields(
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
        client execute {
          bulk(
            bulkStatements
          )
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

    import scala.concurrent.ExecutionContext.Implicits.global

    bulkStatements += i
    bulkCount += 1

    esHelper.clients foreach { client =>
      if (bulkCount >= settings.bulkSize) {
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
      }
    }
    if (bulkCount >= settings.bulkSize) {
      bulkCount = 0
      bulkStatements.clear()
      Thread.sleep(settings.loopDelay)
    }
  }
}

class OutputESWriterFactory extends OutputWriterFactory {
  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputESWriter = {
    new OutputESWriter(model, statusLogger, ElasticsearchHelper, settings)
  }
}
