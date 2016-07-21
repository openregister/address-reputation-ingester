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

import com.github.simplyscala.{MongoEmbedDatabase, MongodProps}
import com.mongodb.casbah.{MongoClient, MongoClientURI, MongoCollection}

import scala.collection.mutable.ListBuffer

class MongoTestConnection(mongodProps: MongodProps,
                          dbName: String = "testDB") extends MongoEmbedDatabase {

  val port = mongodProps.mongodProcess.getConfig.net().getPort
  val uri = s"mongodb://localhost:$port/$dbName"
  val configItem = "mongodb.uri" -> uri
  println(s"connecting to $uri")

  private val collections = new ListBuffer[MongoCollection]()

  private val mongoClient = MongoClient(MongoClientURI(uri))
  val testDB = mongoClient(dbName)

  def collection(name: String): MongoCollection = {
    val c = testDB(name)
    collections += c
    c
  }

  def dropAll() {
    collections.foreach(_.drop())
    collections.clear()
  }

  def stop() {
    mongoClient.close()
    mongoStop(mongodProps)
  }
}
