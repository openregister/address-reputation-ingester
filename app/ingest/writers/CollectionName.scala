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

import config.Divider._

case class CollectionName(productName: String,
                          epoch: Int,
                          index: Option[Int]) {

  def toPrefix: String = s"${productName}_${epoch}"

  override def toString: String = CollectionName.format(productName, epoch, index.get)
}


object CollectionName {
  def apply(collectionName: String): Option[CollectionName] = {
    val parts = qsplit(collectionName, '_')
    if (parts.size == 3) doParseName(parts) else None
  }

  private def doParseName(parts: List[String]): Option[CollectionName] = {
    try {
      Some(CollectionName(
        productName = parts.head,
        epoch = parts(1).toInt,
        index = Some(parts(2).toInt)
      ))
    } catch {
      case n: NumberFormatException => None
    }
  }

  def format(productName: String, epoch: Int, index: Int): String = "%s_%d_%03d".format(productName, epoch, index)
}

