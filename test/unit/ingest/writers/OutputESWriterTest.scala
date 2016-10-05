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

import com.mongodb.casbah.MongoCollection
import com.sksamuel.elastic4s.ElasticClient
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FreeSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar.mock
import services.es.IndexMetadata
import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class OutputESWriterTest extends FreeSpec {

  val ec = scala.concurrent.ExecutionContext.Implicits.global
  val now = new Date()
  val yesterday = new Date(now.getTime - 86400000L)

  class Context(timestamp: String, collectionNames: Set[String]) {
    //    val mongoDB = mock[MongoDB]
    //    val store = mock[MongoSystemMetadataStore]
    val esClient = mock[ElasticClient]
    val indexMetadata = mock[IndexMetadata]
    //    val bulk = mock[BulkWriteOperation]
    val logger = new StubLogger()
    val model = new StateModel(productName = "x", epoch = 4, timestamp = Some(timestamp))
    val status = new StatusLogger(logger)

    //    when(collectionMetadata.db) thenReturn mongoDB
    //    when(collectionMetadata.systemMetadata) thenReturn store
    when(indexMetadata.clients) thenReturn List(esClient)
    when(indexMetadata.existingCollectionNames) thenReturn collectionNames.toList.sorted

    val all = collectionNames + ("x_4_" + timestamp)
    val collections = all.map(n => n -> mock[MongoCollection]).toMap

    for (n <- all) {
      val collection = collections(n)
      //      when(mongoDB.collectionExists(n)) thenReturn true
      //      when(mongoDB(n)) thenReturn collection
      when(collection.name) thenReturn n
      //      when(collection.initializeUnorderedBulkOperation) thenReturn bulk
    }
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
      "then targetExistsAndIsNewerThan will return None" ignore {
        //TODO
      }
    }


    "when the model has corresponding collections with old completion dates" - {
      "then targetExistsAndIsNewerThan will return None" ignore {
        //TODO
      }
    }

    "when the model has corresponding collections with newish completion dates" - {
      "then targetExistsAndIsNewerThan will return the last collection name" ignore {
        //TODO
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
}
