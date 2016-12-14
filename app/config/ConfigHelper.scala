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

package config

import com.google.inject.{Inject, Singleton}
import play.api.{Configuration, Environment}

import scala.collection.JavaConverters._

@Singleton
class ConfigHelper @Inject() (config: Configuration, env: Environment) {
  
  val mode = env.mode

  def mustGetConfigString(key: String): String = {
    getConfigString(key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def mustGetConfigStringList(key: String): List[String] = {
    getConfigStringList(key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def mustGetConfigInt(key: String): Int = {
    getConfigInt(key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def getConfigString(key: String): Option[String] = {
    val modeKey = s"$mode.$key"
    config.getString(modeKey).orElse(config.getString(key))
  }

  def getConfigStringList(key: String): Option[List[String]] = {
    val modeKey = s"$mode.$key"
    config.getStringList(modeKey).orElse(config.getStringList(key)).map(_.asScala.toList)
  }

  def getConfigInt(key: String): Option[Int] = {
    val modeKey = s"$mode.$key"
    config.getInt(modeKey).orElse(config.getInt(key))
  }

  def getConfig(key: String): Option[Configuration] = {
    val modeKey = s"$mode.$key"
    config.getConfig(modeKey).orElse(config.getConfig(key))
  }

  def replaceHome(string: String): String = {
    if (string.startsWith("$HOME")) System.getenv("HOME") + string.substring(5)
    else string
  }
}
