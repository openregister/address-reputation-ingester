package config

import uk.gov.hmrc.http._
import uk.gov.hmrc.http.hooks.HttpHooks
import uk.gov.hmrc.play.audit.http.HttpAuditing
import uk.gov.hmrc.play.audit.http.connector.AuditConnector
import uk.gov.hmrc.play.auth.microservice.connectors.AuthConnector
import uk.gov.hmrc.play.config.{AppName, RunMode, ServicesConfig}
import uk.gov.hmrc.play.http.ws._
import uk.gov.hmrc.http.hooks.HttpHook
import uk.gov.hmrc.play.microservice.config.LoadAuditingConfig

trait Hooks extends HttpHooks {
  override val hooks = NoneRequired
}

trait WSHttp extends HttpGet with WSGet with HttpPut with WSPut with HttpPost with WSPost with HttpDelete with WSDelete with Hooks with AppName
object WSHttp extends WSHttp


object MicroserviceAuditConnector extends AuditConnector with RunMode {
  override lazy val auditingConfig = LoadAuditingConfig(s"$env.auditing")
}

object MicroserviceAuthConnector extends AuthConnector with ServicesConfig with WSHttp {
  override val authBaseUrl = baseUrl("auth")
}
