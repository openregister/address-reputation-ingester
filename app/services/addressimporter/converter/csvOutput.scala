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

package services.addressimporter.converter

case class CSVLine(uprn: Long, line1: String, line2: String, line3: String, town: String, postcode: String) {
  import OSCleanup._

  override def toString: String =
    uprn + "," +
      "\"" + line1.rmDupSpace.capitalisation.trim + "\"" + "," +
      "\"" + line2.rmDupSpace.capitalisation.trim + "\"" + "," +
      "\"" + line3.rmDupSpace.capitalisation.trim + "\"" + "," +
      "\"" + town.rmDupSpace.capitalisation.trim + "\"" + "," +
      postcode.trim
}
