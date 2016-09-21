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

package services.model

import fetch.OSGBProduct
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import services.mongo.CollectionName

case class StateModel(
                       productName: String = "",
                       epoch: Int = 0,
                       variant: Option[String] = None,
                       timestamp: Option[String] = None,
                       product: Option[OSGBProduct] = None,
                       target: String = "db",
                       forceChange: Boolean = false,
                       hasFailed: Boolean = false
                     ) {

  def pathSegment: String = {
    val v = variant getOrElse "full"
    s"$productName/$epoch/$v"
  }

  def withNewTimestamp: StateModel = {
    // note: no underscores (would break our logic) and no dashes (they are problematic in Mongo)
    val formatter = DateTimeFormat.forPattern("yyyyMMddHHmm")
    val timestamp = formatter.print(new DateTime())
    copy(timestamp = Some(timestamp))
  }

  def collectionName: CollectionName = CollectionName(productName, Some(epoch), timestamp)
}


object StateModel {
  def apply(product: OSGBProduct): StateModel = {
    new StateModel(product.productName, product.epoch, None, None, Some(product))
  }

  def apply(collectionName: CollectionName): StateModel = {
    new StateModel(collectionName.productName, collectionName.epoch.get, None, collectionName.timestamp, None)
  }
}
