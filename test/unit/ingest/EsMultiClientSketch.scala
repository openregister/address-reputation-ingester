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

import java.io.File

import services.model.{StateModel, StatusLogger}
import uk.gov.hmrc.BuildProvenance
import uk.gov.hmrc.address.osgb.DbAddress
import uk.gov.hmrc.address.services.es.{ESAdminImpl, ElasticDiskClientSettings, ElasticsearchHelper, IndexMetadata}
import uk.gov.hmrc.address.services.writers.{OutputESWriter, WriterSettings}
import uk.gov.hmrc.logging.Stdout
import uk.gov.hmrc.util.FileUtils

// for manual test/development
object EsMultiClientSketch {
  private val ec = play.api.libs.concurrent.Execution.Implicits.defaultContext

  def main(args: Array[String]) {
    val model = StateModel("essay", Some(1), None, Some("ts1"), None, "es")
    //.withNewTimestamp
    val status = new StatusLogger(Stdout)

    val esDataPath = System.getProperty("java.io.tmpdir") + "/es"
    val diskEsClient = ElasticsearchHelper.buildDiskClient(ElasticDiskClientSettings(esDataPath, true))
    val diskEsImpl = new ESAdminImpl(List(diskEsClient), Stdout, ec)
    val diskIndexMetadata = new IndexMetadata(diskEsImpl, false, Map("essay" -> 2), status, ec)
    val w = new OutputESWriter(model, status, diskIndexMetadata, WriterSettings.default, ec, BuildProvenance(None, None))

    println("Before: " + diskIndexMetadata.existingIndexes)
    println("begin...")

    w.begin()
    w.output(DbAddress("a1", List("1 High St"), Some("Town"), "NE1 1AA", Some("GB-ENG"), Some("UK"), Some(1234),
      Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0")))
    w.end(true)

    println("ended.")
    println("From disk:  " + diskIndexMetadata.existingIndexes)
    println(diskIndexMetadata.findMetadata(model.indexName))

    Thread.sleep(1000)

    val localEsClient = ElasticsearchHelper.buildNodeLocalClient()
    val localEsImpl = new ESAdminImpl(List(localEsClient), Stdout, ec)
    val localIndexMetadata = new IndexMetadata(localEsImpl, false, Map("essay" -> 2), status, ec)

    println("From local: " + localIndexMetadata.existingIndexes)
    println(localIndexMetadata.findMetadata(model.indexName))
  }
}
