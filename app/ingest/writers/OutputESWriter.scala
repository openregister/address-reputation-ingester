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
import config.ApplicationGlobal
import services.es.{ESSchema, IndexMetadata}
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.address.osgb.DbAddress

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class OutputESWriter(var model: StateModel, statusLogger: StatusLogger, indexMetadata: IndexMetadata,
                     settings: WriterSettings, ec: ExecutionContext) extends OutputWriter with ElasticDsl {

  private implicit val x = ec
  private val address = indexMetadata.address
  private val metadata = indexMetadata.metadata
  private val indexName = model.collectionName.toString

  override def existingTargetThatIsNewerThan(date: Date): Option[String] = {
    val similar = indexMetadata.existingCollectionNamesLike(model.collectionName)
    val found = similar.reverse.find {
      name =>
        val info = indexMetadata.findMetadata(name)
        info.exists(_.completedAfter(date))
    }
    found.map(_.toString)
  }

  override def begin() {
    if (indexMetadata.collectionExists(indexName))
      indexMetadata.dropCollection(indexName)

    indexMetadata.writeIngestSettingsTo(indexName, settings)

    indexMetadata.clients foreach { client =>
      client execute {
        ESSchema.createIndexDefinition(indexName, address, metadata,
          ESSchema.Settings(indexMetadata.numShards(model.productName), 0, "60s"))
      } await
    }
  }

  override def output(a: DbAddress) {
    val at = a.tupledFlat.toMap + ("id" -> a.id)
    addBulk(
      index into indexName -> address fields at id a.id routing a.postcode
    )
  }

  override def end(completed: Boolean): StateModel = {
    indexMetadata.clients foreach { client =>
      if (bulkCount != 0) {
        val fr = client execute {
          bulk(
            bulkStatements
          )
        }

        Await.ready(fr, Duration.Inf) foreach { br =>
          if (br.hasFailures) {
            statusLogger.warn(s"Elasticsearch failure processing bulk insertion - ${br.failureMessage}")
            model = model.copy(hasFailed = true)
            throw new Exception(s"Elasticsearch failure processing bulk insertion - ${br.failureMessage}")
          }
        }
      }

      if (completed) {
        client execute {
          update settings indexName set Map(
            "index.refresh_interval" -> "1s"
          )
        } await

        // we have finished! let's celebrate
        indexMetadata.writeCompletionDateTo(indexName)
      }

    }
    statusLogger.info(s"Finished ingesting to index $indexName")
    model
  }

  private var bulkCount = 0
  private var bulkStatements = collection.mutable.Buffer[IndexDefinition]()

  private def addBulk(i: IndexDefinition) = {
    bulkStatements += i
    bulkCount += 1

    if (bulkCount >= settings.bulkSize) {
      val fr = indexMetadata.clients map { client =>
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
  def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings, ec: ExecutionContext): OutputESWriter = {
    new OutputESWriter(model, statusLogger, ApplicationGlobal.elasticSearchService, settings, ec)
  }
}
