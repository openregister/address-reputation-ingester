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
import services.es.{ElasticsearchHelper, IndexMetadata}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.logging.Stdout

// for manual test/development
object ElasticsearchSketch {
  implicit val ec = scala.concurrent.ExecutionContext.Implicits.global

  def main(args: Array[String]) {
    val model = StateModel("essay", 1, None, Some("ts1"), None, "es") //.withNewTimestamp
    val status = new StatusLogger(Stdout)
    val esHelper = ElasticsearchHelper("elasticsearch", "elasticsearch://localhost:9300", false)
    val w = new OutputESWriter(model, status, esHelper, WriterSettings.default)
    val im = new IndexMetadata(esHelper)
    println(im.existingCollectionNames)
    w.begin()
    w.output(DbAddress("a1", List("1 High St"), Some("Town"), "NE1 1AA", Some("GB-ENG")))
    w.end(true)

    println(im.existingCollectionNames)
    println(im.findMetadata(model.collectionName))
  }
}
