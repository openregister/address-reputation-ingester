/*
 *
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
 *
 *
 */

package object addressbase {

  def blankToOption(s: String): Option[String] = if (s.isEmpty) None else Some(s)

  def blankToOptInt(s: String): Option[Int] = if (s.isEmpty) None else Some(s.toInt)

  def blankToOptLong(s: String): Option[Long] = if (s.isEmpty) None else Some(s.toLong)

  def optIntToString(v: Option[Int]): String = if (v.isDefined) v.get.toString else ""

  def optLongToString(v: Option[Long]): String = if (v.isDefined) v.get.toString else ""

  def blankToChar(s: String): Char = if (s.nonEmpty) s.charAt(0) else '\u0000'

}
