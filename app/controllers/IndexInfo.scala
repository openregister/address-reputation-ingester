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

case class ListCI(indexes: List[IndexInfo])


case class IndexInfo(
                      name: String,
                      size: Option[Int],
                      system: Boolean,
                      inUse: Boolean,
                      doNotDelete: Boolean = false,
                      completedAt: Option[String] = None,
                      bulkSize: Option[String] = None,
                      loopDelay: Option[String] = None,
                      includeDPA: Option[String] = None,
                      includeLPI: Option[String] = None,
                      prefer: Option[String] = None,
                      streetFilter: Option[String] = None,
                      buildVersion: Option[String] = None,
                      buildNumber: Option[String] = None,
                      aliases: List[String] = Nil
                    )


object IndexInfo {

  implicit val IndexInfoReads: Reads[IndexInfo] = (
    (JsPath \ "name").read[String] and
      (JsPath \ "size").readNullable[Int] and
      (JsPath \ "system").read[Boolean] and
      (JsPath \ "inUse").read[Boolean] and
      (JsPath \ "doNotDelete").read[Boolean] and
      (JsPath \ "completedAt").readNullable[String] and
      (JsPath \ "bulkSize").readNullable[String] and
      (JsPath \ "loopDelay").readNullable[String] and
      (JsPath \ "includeDPA").readNullable[String] and
      (JsPath \ "includeLPI").readNullable[String] and
      (JsPath \ "prefer").readNullable[String] and
      (JsPath \ "streetFilter").readNullable[String] and
      (JsPath \ "buildVersion").readNullable[String] and
      (JsPath \ "buildNumber").readNullable[String] and
      (JsPath \ "aliases").read[List[String]]) (IndexInfo.apply _)

  implicit val IndexInfoWrites: Writes[IndexInfo] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "size").writeNullable[Int] and
      (JsPath \ "system").write[Boolean] and
      (JsPath \ "inUse").write[Boolean] and
      (JsPath \ "doNotDelete").write[Boolean] and
      (JsPath \ "completedAt").writeNullable[String] and
      (JsPath \ "bulkSize").writeNullable[String] and
      (JsPath \ "loopDelay").writeNullable[String] and
      (JsPath \ "includeDPA").writeNullable[String] and
      (JsPath \ "includeLPI").writeNullable[String] and
      (JsPath \ "prefer").writeNullable[String] and
      (JsPath \ "streetFilter").writeNullable[String] and
      (JsPath \ "buildVersion").writeNullable[String] and
      (JsPath \ "buildNumber").writeNullable[String] and
      (JsPath \ "aliases").write[List[String]]) (unlift(IndexInfo.unapply))

  implicit val ListIndexInfoWrites: Writes[ListCI] = Json.format[ListCI]
}

