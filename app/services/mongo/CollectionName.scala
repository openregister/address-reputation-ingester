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

package services.mongo

import config.Divider._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

// Normal collections will have all fields defined.
// However, we allow for absent fields primarily for backward compatibility.

case class CollectionName(productName: String,
                          epoch: Option[Int],
                          timestamp: Option[String] = None) {

  def toPrefix: String = s"${productName}_${epoch.get}"

  override lazy val toString: String =
    if (timestamp.isDefined) CollectionName.format(productName, epoch.get, timestamp.get)
    else if (epoch.isDefined) toPrefix
    else productName
}


object CollectionName {
  def apply(collectionName: String): Option[CollectionName] =
    if (collectionName.isEmpty) None
    else {
      val parts = qsplit(collectionName, '_')
      if (parts.size <= 3) doParseName(parts) else None
    }

  private def doParseName(parts: List[String]): Option[CollectionName] = {
    try {
      val epoch = if (parts.size >= 2) Some(parts(1).toInt) else None
      val timestamp = if (parts.size >= 3) Some(parts(2)) else None
      Some(CollectionName(parts.head, epoch, timestamp))
    } catch {
      case n: NumberFormatException => None
    }
  }

  def format(productName: String, epoch: Int, timestamp: String): String = "%s_%d_%s".format(productName, epoch, timestamp)
}

