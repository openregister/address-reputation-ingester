/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package services.audit

import config.AppVersion
import play.api.Environment
import com.google.inject.{Inject, Singleton}
import uk.gov.hmrc.play.audit.http.config.LoadAuditingConfig
import uk.gov.hmrc.play.audit.http.connector.AuditConnector
import uk.gov.hmrc.play.audit.model.{Audit, DataEvent, EventTypes}
import uk.gov.hmrc.play.config.AppName


@Singleton
class AuditClient @Inject()(env: Environment) extends AppName with AppVersion {

  val audit = Audit(appName, new AuditConnector {
    override lazy val auditingConfig = LoadAuditingConfig(s"${env.mode}.auditing")
  })

  def succeeded(detail: Map[String, String]) {
    audit.sendDataEvent(
      DataEvent(
        auditSource = appName,
        auditType = EventTypes.Succeeded,
        detail = detail ++ Map("appVersion" -> appVersion))
    )
  }

}
