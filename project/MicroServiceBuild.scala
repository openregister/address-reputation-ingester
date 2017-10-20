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

import sbt._

object MicroServiceBuild extends Build with MicroService {
  import scala.util.Properties.envOrElse

  val appName = "address-reputation-ingester"
  val appVersionKey = appName.toUpperCase.replace('-', '_') + "_VERSION"
  val appVersion = envOrElse(appVersionKey, "999-SNAPSHOT")

  override lazy val appDependencies: Seq[ModuleID] = AppDependencies()
}

private object AppDependencies {
  import play.sbt.PlayImport._
  import play.core.PlayVersion

  private val hmrcTestVersion = "3.0.0"
  private val pegdownVersion = "1.6.0"
  private val jacksonVersion = "2.7.4"
  private val scalaTestPlusPlayVersion = "2.0.1"
  private val scalacticVersion = "3.0.1"

  val compile = Seq(

    ws excludeAll ExclusionRule(organization = "commons-logging"),
    "uk.gov.hmrc" %% "microservice-bootstrap" % "6.10.0",
    "uk.gov.hmrc" %% "play-url-binders" % "2.1.0",
    "uk.gov.hmrc" %% "address-reputation-store" % "2.29.0" withSources(),
    "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.4.0",
    "com.github.lookfirst" % "sardine" % "5.7",
    "net.openhft" % "chronicle-map" % "3.8.0",
    "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
    "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
    "info.debatty" % "java-string-similarity" % "0.18"
  )

  trait TestDependencies {
    lazy val scope: String = "test"
    lazy val test : Seq[ModuleID] = ???
  }

  object Test {
    def apply() = new TestDependencies {
      override lazy val test = Seq(
        "uk.gov.hmrc" %% "hmrctest" % hmrcTestVersion % scope,
        "org.pegdown" % "pegdown" % pegdownVersion % scope,
        "com.typesafe.play" %% "play-test" % PlayVersion.current % scope,
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % scope,
        "org.scalatestplus.play" %% "scalatestplus-play" % scalaTestPlusPlayVersion % scope,
        "org.mockito" % "mockito-all" % "1.10.19" % scope,
        "com.pyruby" % "java-stub-server" % "0.14" % scope,
        "org.scalactic" %% "scalactic" % scalacticVersion force()
      )
    }.test
  }

  object IntegrationTest {
    def apply() = new TestDependencies {

      override lazy val scope: String = "it"

      override lazy val test = Seq(
        "uk.gov.hmrc" %% "hmrctest" % hmrcTestVersion % scope,
        //"org.scalatest" %% "scalatest" % scalaTestVersion % scope,
        "org.pegdown" % "pegdown" % pegdownVersion % scope,
        "com.typesafe.play" %% "play-test" % PlayVersion.current % scope,
        "org.scalamock" %% "scalamock-scalatest-support" % "3.2" % scope,
        "org.scalatestplus.play" %% "scalatestplus-play" % scalaTestPlusPlayVersion % scope,
        "org.mockito" % "mockito-all" % "1.10.19" % scope,
        "com.pyruby" % "java-stub-server" % "0.14" % scope,
        "io.milton" % "milton-server-ce" % "2.7.1.5" % scope,
        "org.scalactic" %% "scalactic" % scalacticVersion force()
      )
    }.test
  }

  def apply() = compile ++ Test() ++ IntegrationTest()
}

