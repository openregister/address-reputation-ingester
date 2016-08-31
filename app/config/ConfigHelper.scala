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

import java.util

import play.api.Configuration
import play.api.Mode._

import scala.collection.JavaConverters._
import scala.collection.mutable

object ConfigHelper {
  def mustGetConfigString(config: Configuration, key: String): String = {
    getConfigString(config, key).getOrElse {
      throw new Exception("ERROR: Unable to find config item " + key)
    }
  }

  def mustGetConfigString(mode: Mode, config: Configuration, key: String): String = {
    getConfigString(mode, config, key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def mustGetConfigStringList(config: Configuration, key: String): List[String] = {
    getConfigStringList(config, key).getOrElse {
      throw new Exception("ERROR: Unable to find config item " + key)
    }
  }

  def mustGetConfigStringList(mode: Mode, config: Configuration, key: String): List[String] = {
    getConfigStringList(mode, config, key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def mustGetConfigInt(config: Configuration, key: String): Int = {
    getConfigInt(config, key).getOrElse {
      throw new Exception("ERROR: Unable to find config item " + key)
    }
  }

  def mustGetConfigInt(mode: Mode, config: Configuration, key: String): Int = {
    getConfigInt(mode, config, key).getOrElse {
      throw new Exception(s"ERROR: Unable to find config item $mode.$key or $key")
    }
  }

  def getConfigString(config: Configuration, key: String): Option[String] = config.getString(key)

  def getConfigString(mode: Mode, config: Configuration, key: String): Option[String] = {
    val modeKey = s"$mode.$key"
    config.getString(modeKey).orElse(config.getString(key))
  }

  def getConfigStringList(config: Configuration, key: String): Option[List[String]] = config.getStringList(key).map(_.asScala.toList)

  def getConfigStringList(mode: Mode, config: Configuration, key: String): Option[List[String]] = {
    val modeKey = s"$mode.$key"
    config.getStringList(modeKey).orElse(config.getStringList(key)).map(_.asScala.toList)
  }

  def getConfigInt(config: Configuration, key: String): Option[Int] = config.getInt(key)

  def getConfigInt(mode: Mode, config: Configuration, key: String): Option[Int] = {
    val modeKey = s"$mode.$key"
    config.getInt(modeKey).orElse(config.getInt(key))
  }

  def getConfigBoolean(config: Configuration, key: String): Option[Boolean] = config.getBoolean(key)

  def getConfigBoolean(mode: Mode, config: Configuration, key: String): Option[Boolean] = {
    val modeKey = s"$mode.$key"
    config.getBoolean(modeKey).orElse(config.getBoolean(key))
  }

  def replaceHome(string: String): String = {
    if (string.startsWith("$HOME")) System.getenv("HOME") + string.substring(5)
    else string
  }
}
