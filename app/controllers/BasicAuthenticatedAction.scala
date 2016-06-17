/*
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
 */

package controllers

import config.ConfigHelper._
import play.api.Configuration
import play.api.Mode.Mode
import play.api.http.HeaderNames.{AUTHORIZATION, WWW_AUTHENTICATE}
import play.api.mvc.{Result, _}
import uk.gov.hmrc.secure.{PasswordHasher, Salt, Scrambled}

import scala.concurrent.Future

class PassThroughAction extends ActionBuilder[Request] with ActionFilter[Request] {

  def filter[A](request: Request[A]): Future[Option[Result]] = Future.successful(None)
}


class BasicAuthenticatedAction(authConfig: BasicAuthenticationFilterConfiguration)
  extends ActionBuilder[Request]
    with ActionFilter[Request] {

  def filter[A](request: Request[A]): Future[Option[Result]] = Future.successful(doFilter(request))

  private def doFilter[A](request: Request[A]): Option[Result] = {
    if (!authConfig.enabled)
      None // bypassed
    else {
      val creds = BasicAuthCredentials.fromAuthorizationHeader(request.headers.get(AUTHORIZATION))
      if (authConfig.matches(creds))
        None // valid credentials: allow subsequent operations to proceed
      else
        Some(Results.Unauthorized.withHeaders(WWW_AUTHENTICATE -> authConfig.basicRealm))
    }
  }
}


case class BasicAuthenticationFilterConfiguration(realm: String, enabled: Boolean,
                                                  username: String, password: Scrambled, salt: Salt) {
  val basicRealm = "Basic realm=\"" + realm + "\""
  private val hasher = new PasswordHasher().withSalt(salt)

  def matches(other: Option[BasicAuthCredentials]): Boolean = {
    other match {
      case None =>
        false
      case Some(bac) =>
        username == bac.username && password == hasher.hash(bac.conjoined)
    }
  }
}


object BasicAuthenticationFilterConfiguration {
  def parse(mode: Mode, configuration: Configuration): BasicAuthenticationFilterConfiguration = {
    def key(k: String) = s"basicAuthentication.$k"

    val enabled = mustGetConfigString(mode, configuration, key("enabled")).toBoolean
    val realm = mustGetConfigString(mode, configuration, key("realm"))
    val username = mustGetConfigString(mode, configuration, key("username"))
    val hashedPassword = Scrambled(mustGetConfigString(mode, configuration, key("password")))
    val salt = Salt(mustGetConfigString(mode, configuration, key("salt")))

    BasicAuthenticationFilterConfiguration(
      realm,
      enabled,
      username,
      hashedPassword,
      salt
    )
  }
}
