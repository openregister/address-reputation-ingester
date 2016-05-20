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

package controllers

import play.api.libs.functional.syntax._
import play.api.libs.json.{JsPath, Json, Reads, Writes}

case class ListCI(collections: List[CollectionInfo])


case class CollectionInfo(name: String, size: Int, system: Boolean, inUse: Boolean,
                          createdAt: Option[String] = None,
                          completedAt: Option[String] = None)


object CollectionInfo {

  implicit val CollectionInfoReads: Reads[CollectionInfo] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "size").read[Int] and
      (JsPath \ "system").read[Boolean] and
      (JsPath \ "inUse").read[Boolean] and
      (JsPath \ "createdAt").readNullable[String] and
      (JsPath \ "completedAt").readNullable[String]) (CollectionInfo.apply _)

  implicit val CollectionInfoWrites: Writes[CollectionInfo] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "size").write[Int] and
      (JsPath \ "system").write[Boolean] and
      (JsPath \ "inUse").write[Boolean] and
      (JsPath \ "createdAt").writeNullable[String] and
      (JsPath \ "completedAt").writeNullable[String]) (unlift(CollectionInfo.unapply))

  implicit val ListCollectionInfoWrites: Writes[ListCI] = Json.format[ListCI]
}

