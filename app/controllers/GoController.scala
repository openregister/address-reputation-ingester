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

package controllers

import controllers.SimpleValidator._
import play.api.mvc.{Action, AnyContent, Request}
import services.ingester.writers.WriterSettings
import uk.gov.hmrc.play.microservice.controller.BaseController

object GoController extends GoController

class GoController extends BaseController {

  def go(product: String, epoch: String, variant: String): Action[AnyContent] = Action {
    request =>
      val settings = WriterSettings(1, 0)
      handleGo(request, product, epoch, variant, settings)
      Ok("")
  }

  private[controllers] def handleGo(request: Request[AnyContent],
                                    product: String, epoch: String, variant: String,
                                    settings: WriterSettings
                                   ) = {
    require(isAlphaNumeric(product))
    require(isAlphaNumeric(epoch))
    require(isAlphaNumeric(variant))
    ""
  }
}
