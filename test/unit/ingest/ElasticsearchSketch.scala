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

package ingest

import ingest.writers.{OutputESWriter, WriterSettings}
import services.es.ElasticsearchHelper
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.logging.Stdout

// for manual test/development
object ElasticsearchSketch {
  val ec = scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]) {
    val model = StateModel("essay", 1, None, Some("ts1"), None, "es") //.withNewTimestamp
    val status = new StatusLogger(Stdout)
    val indexMetadata = ElasticsearchHelper("elasticsearch", "elasticsearch://localhost:9300", false, Map("essay" -> 2), ec, Stdout)
    val w = new OutputESWriter(model, status, indexMetadata, WriterSettings.default, ec)
    println(indexMetadata.existingCollectionNames)
    w.begin()
    w.output(DbAddress("a1", List("1 High St"), Some("Town"), "NE1 1AA", Some("GB-ENG"), Some("UK"), Some(1234),
      Some("en"), Some(2), Some(1), Some(8)))
    w.end(true)

    println(indexMetadata.existingCollectionNames)
    println(indexMetadata.findMetadata(model.collectionName))
  }
}
