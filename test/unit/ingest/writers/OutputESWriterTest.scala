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

package ingest.writers

import java.util.Date

import com.sksamuel.elastic4s.ElasticClient
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.es.IndexMetadata
import services.model.{StateModel, StatusLogger}
import services.mongo.{CollectionMetadataItem, CollectionName}
import uk.gov.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class OutputESWriterTest extends FreeSpec {

  val ec = scala.concurrent.ExecutionContext.Implicits.global
  val now = new Date()
  val yesterday = new Date(now.getTime - 86400000L)

  val x_1_ts1 = CollectionName("x_1_ts1").get
  val x_4_ts1 = CollectionName("x_4_ts1").get
  val x_4_ts2 = CollectionName("x_4_ts2").get

  class Context(timestamp: String, collectionNames: Set[String]) {
    val esClient = mock[ElasticClient]
    val indexMetadata = mock[IndexMetadata]
    val logger = new StubLogger()
    val model = new StateModel(productName = "x", epoch = 4, timestamp = Some(timestamp))
    val status = new StatusLogger(logger)

    when(indexMetadata.clients) thenReturn List(esClient)
    when(indexMetadata.existingCollectionNames) thenReturn collectionNames.toList.sorted
  }

  "targetExistsAndIsNewerThan" - {
    "when the model has no corresponding collection yet" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts1", Set("admin", "x_1_ts1", "x_2_ts1", "x_3_ts1")) {
          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0), ec)

          val result = outputESWriter.existingTargetThatIsNewerThan(new Date())

          assert(result === None)
        }
      }
    }


    "when the model has corresponding collections without any completion dates" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          when(indexMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(indexMetadata.findMetadata(x_4_ts1)) thenReturn None
          when(indexMetadata.findMetadata(x_4_ts2)) thenReturn None
          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0), ec)

          val result = outputESWriter.existingTargetThatIsNewerThan(yesterday)

          assert(result === None)
        }
      }
    }

    "when the model has corresponding collections with old completion dates" - {
      "then targetExistsAndIsNewerThan will return None" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          when(indexMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(indexMetadata.findMetadata(x_4_ts1)) thenReturn None
          when(indexMetadata.findMetadata(x_4_ts2)) thenReturn None

          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0), ec)

          val result = outputESWriter.existingTargetThatIsNewerThan(now)

          assert(result === None)
        }
      }
    }

    "when the model has corresponding collections with newish completion dates" - {
      "then targetExistsAndIsNewerThan will return the last collection name" in {
        new Context("ts3", Set("admin", "x_4_ts1", "x_4_ts2")) {
          val now = new Date()
          val yesterday = new Date(now.getTime - 86400000L)

          when(indexMetadata.existingCollectionNames) thenReturn List("admin", "x_4_ts1", "x_4_ts2")
          when(indexMetadata.findMetadata(x_4_ts1)) thenReturn Some(CollectionMetadataItem(x_4_ts1, 10, None, Some(dateAgo(864000000))))
          when(indexMetadata.findMetadata(x_4_ts2)) thenReturn Some(CollectionMetadataItem(x_4_ts2, 10, None, Some(dateAgo(1000))))

          val outputESWriter = new OutputESWriter(model, status, indexMetadata, WriterSettings(10, 0), ec)

          val result = outputESWriter.existingTargetThatIsNewerThan(yesterday)

          assert(result === Some("x_4_ts2"))
        }
      }
    }
  }

  "output" - {
    "when a DbAddress is passed to the writer" - {
      """
         then an insert is invoked
         and the collection name is chosen correctly
      """ ignore {
        //TODO
      }
    }
  }

  "end" - {
    "when close is called on the writer" - {
      """
         then a completion timestamp document is written to the output collection
         and an index is created for the postcode field
         and then close is called on the mongoDB instance
      """ ignore {
        //TODO
      }
    }
  }

  private def dateAgo(ms: Long) = {
    val now = System.currentTimeMillis
    new Date(now - ms)
  }
}
