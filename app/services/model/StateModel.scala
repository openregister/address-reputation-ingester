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

import services.fetch.OSGBProduct

case class StateModel(
                       product: String = "",
                       epoch: Int = 0,
                       variant: String = "",
                       index: Option[Int] = None,
                       products: List[OSGBProduct] = Nil,
                       hasFailed: Boolean = false
                     ) {

  def pathSegment: String = s"${product}/${epoch}/${variant}"

  def collectionBaseName: String = s"${product}_${epoch}"

  def collectionName: Option[String] = index.map(i => s"${collectionBaseName}_$i")
}

