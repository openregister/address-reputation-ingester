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

package helper

import com.github.simplyscala.MongoEmbedDatabase
import controllers.{IngestControllerITest, PingTest}
import org.scalatest.{Args, Status, Suite}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection

object EmbeddedMongoSuite extends Suite with MongoEmbedDatabase {

  // Currently, we have to set the number of test suites explicitly (pah!)
  //------------------------------------------------
  var HowManyTestSuitesAreUsingThisServerWrapper =
    Set(
      classOf[PingTest],
      classOf[IngestControllerITest]
    ).size
  //------------------------------------------------

  lazy val mongoTestConnection = new MongoTestConnection(mongoStart())

  def stop() {
    HowManyTestSuitesAreUsingThisServerWrapper -= 1
    if (HowManyTestSuitesAreUsingThisServerWrapper <= 0) {
      mongoTestConnection.stop()
    }
  }
}


trait EmbeddedMongoSuite extends Suite {

  override def run(testName: Option[String], args: Args): Status = {
    try {
      val status = super.run(testName, args)
      status.waitUntilCompleted()
      status
    }
    finally {
      EmbeddedMongoSuite.stop()
    }
  }

  def mongoTestConnection = EmbeddedMongoSuite.mongoTestConnection

  def embeddedMongoSettings = Map(mongoTestConnection.configItem)

  def casbahMongoConnection() = new CasbahMongoConnection(mongoTestConnection.uri)
}
