package config

import uk.gov.hmrc.play.audit.http.config.LoadAuditingConfig
import uk.gov.hmrc.play.audit.http.connector.AuditConnector
import uk.gov.hmrc.play.auth.microservice.connectors.AuthConnector
import uk.gov.hmrc.play.config.{AppName, RunMode, ServicesConfig}
import uk.gov.hmrc.play.http.ws._

//object WSHttp extends WSGet with WSPut with WSPost with WSDelete with WSPatch with AppName with RunMode {
//  override val auditConnector = MicroserviceAuditConnector
//}

object WSHttp extends WSGet with WSPut with WSPost with WSDelete with WSPatch with AppName  {
   val hooks = Seq.empty[uk.gov.hmrc.play.http.hooks.HttpHook]
//    val auditConnector = MicroserviceAuditConnector
}

object MicroserviceAuditConnector extends AuditConnector with RunMode {
  lazy val auditingConfig = LoadAuditingConfig(s"$env.auditing")
}

object MicroserviceAuthConnector extends AuthConnector with ServicesConfig {
  override val authBaseUrl = baseUrl("auth")
}
