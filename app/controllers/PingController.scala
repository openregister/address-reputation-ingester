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

import com.google.inject.Inject

import scala.concurrent.Future
import config.Provenance
import play.api.http.MimeTypes
import play.api.mvc.{Action, AnyContent}
import uk.gov.hmrc.play.microservice.controller.BaseController


class PingController @Inject()(provenance: Provenance) extends BaseController {

  def ping(): Action[AnyContent] = Action { request =>
    Ok(provenance.versionInfo).as(MimeTypes.JSON).withHeaders(CACHE_CONTROL -> "no-cache,max-age=0,must-revalidate")
  }

  def exit(): Action[AnyContent] = Action { request =>
    Future.successful {
      Thread.sleep(500)
      System.exit(10)
    }
    Ok("exiting\n").as(MimeTypes.TEXT)
  }
}
