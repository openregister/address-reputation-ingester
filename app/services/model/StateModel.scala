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
import uk.gov.hmrc.address.services.es.{IndexName, IndexState}

case class StateModel(
                       productName: String = "",
                       epoch: Option[Int] = None,
                       variant: Option[String] = None,
                       timestamp: Option[String] = None,
                       product: Option[OSGBProduct] = None,
                       target: String = "db",
                       forceChange: Boolean = false,
                       hasFailed: Boolean = false
                     ) extends IndexState {

  def forceChangeString: String = StateModel.forceChangeString(forceChange)

  def pathSegment: String = {
    val v = variant getOrElse "full"
    s"$productName/${epoch.get}/$v"
  }

  def withNewTimestamp: StateModel = copy(timestamp = Some(IndexName.newTimestamp))

  lazy val indexName: IndexName = IndexName(productName, epoch, timestamp)

  override def toPrefix: String = indexName.toPrefix

  override def formattedName: String = indexName.formattedName
}


object StateModel {
  def apply(product: OSGBProduct): StateModel = {
    new StateModel(product.productName, Some(product.epoch), None, None, Some(product))
  }

  def apply(indexName: IndexName): StateModel = {
    new StateModel(indexName.productName, indexName.epoch, None, indexName.timestamp, None)
  }

  def forceChangeString(forceChange: Boolean): String = if (forceChange) " (forced)" else ""
}
