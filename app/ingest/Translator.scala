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

import com.mongodb.casbah.commons.MongoDBObject
import fetch.WriterSettings
import services.exec.Continuer
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.services.mongo.CasbahMongoConnection
import uk.co.hmrc.logging.Stdout

class Translator(logger: StatusLogger, continuer: Continuer, mongoDbConnection: CasbahMongoConnection, settings: WriterSettings, model: StateModel) {

  private val db = mongoDbConnection.getConfiguredDb

  private val productName = model.productName

  private val blpuCollection = db(productName + "_blpu")
  private val dpaCollection = db(productName + "_dpa")
  private val lpiCollection = db(productName + "_lpi")
  private val sdCollection = db(productName + "_streetdesc")
  private val streetCollection = db(productName + "_street")

  var blpuCount = 0
  var dpaCount = 0
  var lpiCount = 0
  var sdCount = 0
  var streetCount = 0

  def translateDPA() {
    val cursor = dpaCollection.find()
    while (cursor.hasNext) {
      val dpa = cursor.next.asInstanceOf[MongoDBObject]
    }
  }
}


object Translator {
  val continuer = new Continuer {
    override def isBusy: Boolean = true
  }

  val db = new CasbahMongoConnection("mongodb://localhost:27017/addressbase")

  def main(args: Array[String]) {
    val model = StateModel(args(0), args(1).toInt, Some("full"))
    val settings = WriterSettings(1, 0)
    val translator = new Translator(new StatusLogger(Stdout), continuer, db, settings, model)
    translator.translateDPA()
  }
}
