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

import sbt.{ExclusionRule, _}
import play.sbt.PlayImport._
import play.core.PlayVersion

object HmrcBuild extends Build with MicroService {
  import scala.util.Properties.envOrElse

  val appName = "address-reputation-ingester"
  val appVersion = envOrElse("ADDRESS_REPUTATION_INGESTER_VERSION", "999-SNAPSHOT")

  override lazy val appDependencies: Seq[ModuleID] = compile ++ testDependencies ++ itDependencies

  val jacksonVersion = "2.7.4"

  val compile = Seq(
    ws excludeAll(ExclusionRule(organization = "commons-logging")),
    // netty 3.10 has a breaking API change
    "io.netty" % "netty" % "3.10.6.Final" force(),
//    "uk.gov.hmrc" %% "logging" % "0.2.0" withSources(),
    "uk.gov.hmrc" %% "address-reputation-store" % "2.4.0" withSources(),
    "uk.gov.hmrc" %% "microservice-bootstrap" % "5.8.0",
//    "uk.gov.hmrc" %% "play-authorisation" % "3.1.0",
    "uk.gov.hmrc" %% "play-health" % "2.0.0",
    "uk.gov.hmrc" %% "play-config" % "3.0.0",
    "uk.gov.hmrc" %% "logback-json-logger" % "3.1.0",
    "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.3.1",
    // for some reason this dependency is skipping the local cache and forcing resolution from the internet
    // hence the exclusion
    "com.github.lookfirst" % "sardine" % "5.7" excludeAll ExclusionRule(organization = "org.apache.httpcomponents", name = "httpclient"),
    "org.apache.httpcomponents" % "httpclient" % "4.5.1" force(),
    "org.apache.httpcomponents" % "httpcore" % "4.4.3" force(),
    "net.openhft" % "chronicle-map" % "3.8.0",
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    "info.debatty" % "java-string-similarity" % "0.18"
  )

  private def baseTestDependencies(scope: String) = Seq(
    "org.scalatest" %% "scalatest" % "2.2.6" % scope,
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % scope,
    "com.github.simplyscala" %% "scalatest-embedmongo" % "0.2.2" % scope,
    "org.pegdown" % "pegdown" % "1.4.2" % scope,
    "uk.gov.hmrc" %% "hmrctest" % "2.0.0" % scope,
    "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % scope,
    "org.mockito" % "mockito-all" % "1.10.19" % scope,
    "com.pyruby" % "java-stub-server" % "0.14" % scope,
    "io.milton" % "milton-server-ce" % "2.7.1.5" % scope)

  val testDependencies = baseTestDependencies("test")

  val itDependencies = baseTestDependencies("it") ++
    Seq("com.typesafe.play" %% "play-test" % PlayVersion.current % "it")
}

