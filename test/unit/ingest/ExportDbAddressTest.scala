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

package ingest

import addressbase.OSLpi
import ingest.Ingester.{Street, StreetDescriptor}
import ingest.algorithm.Algorithm
import org.scalatest.FunSuite
import uk.co.hmrc.address.osgb.DbAddress
import java.lang.{Character => JChar}

class ExportDbAddressTest extends FunSuite {

  test("exportLPI with no 'line1' values") {
    val cases = List(
      OSLpi(uprn = 1L, logicalStatus = ' ',
        saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
        usrn = 98765L).normalise ->
        DbAddress("GB1", List("The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 2L, logicalStatus = ' ',
        saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
        usrn = 98765L).normalise ->
        DbAddress("GB2", List("Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 3L, logicalStatus = ' ',
        saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
        usrn = 98765L).normalise ->
        DbAddress("GB3", List("1A-2B", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 4L, logicalStatus = ' ',
        saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "Soo",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
        usrn = 98765L).normalise ->
        DbAddress("GB4", List("Soo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 5L, logicalStatus = ' ',
        saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "Soo",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
        usrn = 98765L).normalise ->
        DbAddress("GB5", List("Soo, 1A-2B", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 6L, logicalStatus = ' ',
        saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
        usrn = 98765L).normalise ->
        DbAddress("GB6", List("1A-2B Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 7L, logicalStatus = ' ',
        saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "Soo",
        paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
        usrn = 98765L).normalise ->
        DbAddress("GB7", List("Soo, Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655)),

      OSLpi(uprn = 8L, logicalStatus = ' ',
        saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "Soo",
        paoStartNumber = "10", paoStartSuffix = "C", paoEndNumber = "12", paoEndSuffix = "D", paoText = "Poo",
        usrn = 98765L).normalise ->
        DbAddress("GB8", List("Soo, 1A-2B Poo", "10C-12D The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"), Some(7655))
    )

    for (c <- cases) {
      val streetsMap = new java.util.HashMap[java.lang.Long, JChar]()
      val streetDescsMap = new java.util.HashMap[java.lang.Long, String]()
      streetsMap.put(98765L, Street('1').pack)
      streetDescsMap.put(98765L, StreetDescriptor("The Street", "Locality Name", "Town Name").pack)

      val a = ExportDbAddress.exportLPI(c._1, streetsMap, streetDescsMap, "SE1 9PY", 'E', 7655, Algorithm())
      assert(a === c._2)
    }
  }
}
