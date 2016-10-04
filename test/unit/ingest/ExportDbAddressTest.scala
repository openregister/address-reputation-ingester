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

import addressbase.{OSDpa, OSLpi}
import ingest.Ingester.{Blpu, Street, StreetDescriptor}
import ingest.algorithm.Algorithm
import org.scalatest.FunSuite
import uk.gov.hmrc.address.osgb.DbAddress

class ExportDbAddressTest extends FunSuite {

  case class Case1(lpi: OSLpi, blpu: Blpu, expected: DbAddress)

  test("exportLPI with no 'line1' values") {
    val cases = List(
      Case1(
        OSLpi(uprn = 1L, language = "ENG", logicalState = '1',
          saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB1", List("The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"), Some("UK"),
          Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 2L, language = "ENG", logicalState = '1',
          saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB2", List("Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 3L, language = "ENG", logicalState = '1',
          saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB3", List("1A-2B", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 4L, language = "ENG", logicalState = '1',
          saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "Soo",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB4", List("Soo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 5L, language = "ENG", logicalState = '1',
          saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "Soo",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB5", List("Soo, 1A-2B", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 6L, language = "ENG", logicalState = '1',
          saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB6", List("1A-2B Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 7L, language = "ENG", logicalState = '1',
          saoStartNumber = "", saoStartSuffix = "", saoEndNumber = "", saoEndSuffix = "", saoText = "Soo",
          paoStartNumber = "", paoStartSuffix = "", paoEndNumber = "", paoEndSuffix = "", paoText = "Poo",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB7", List("Soo, Poo", "The Street", "Locality Name"), Some("Town Name"), "SE1 9PY", Some("GB-ENG"),
          Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0"))),

      Case1(
        OSLpi(uprn = 8L, language = "ENG", logicalState = '1',
          saoStartNumber = "1", saoStartSuffix = "A", saoEndNumber = "2", saoEndSuffix = "B", saoText = "Soo",
          paoStartNumber = "10", paoStartSuffix = "C", paoEndNumber = "12", paoEndSuffix = "D", paoText = "Poo",
          usrn = 98765L).normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB8", List("Soo, 1A-2B Poo", "10C-12D The Street", "Locality Name"), Some("Town Name"), "SE1 9PY",
          Some("GB-ENG"), Some("UK"), Some(7655), Some("en"), Some(2), Some(1), Some(8), None, Some("1.0,-1.0")))
    )

    for (c <- cases) {
      val street = Street('1', "8")
      val streetDesc = StreetDescriptor("The Street", "Locality Name", "Town Name")

      val a = ExportDbAddress.exportLPI(c.lpi, c.blpu, street, streetDesc, Algorithm())
      assert(a === c.expected)
    }
  }


  case class Case2(dpa: OSDpa, blpu: Blpu, expected: DbAddress)

  test("exportDPA with no 'line1' values") {
    val cases = List(
      Case2(
        OSDpa(uprn = 1L,
          subBuildingName = "subBuildingName", buildingName = "buildingName", buildingNumber = "buildingNumber",
          dependentThoroughfareName = "dependentThoroughfareName",
          thoroughfareName = "thoroughfareName",
          doubleDependentLocality = "doubleDependentLocality",
          dependentLocality = "dependentLocality",
          postTown = "postTown",
          postcode = "postcode").normalise,
        Blpu(None, "SE1 9PY", '1', '2', 'E', 7655, "1.0,-1.0"),
        DbAddress("GB1", List("Subbuildingname, Buildingname", "Buildingnumber Dependentthoroughfarename Thoroughfarename",
          "Doubledependentlocality Dependentlocality"), Some("Posttown"), "POSTCODE", Some("GB-ENG"), Some("UK"),
          Some(7655), Some("en"), Some(2), Some(1), None, None, Some("1.0,-1.0"))),

      Case2(
        // 21,"I",52302,10023119082,1,2,2010-09-17,10023119042,291232.36,094165.57,50.7368747,-3.5427382,2,1110,"E",2010-09-18,,2016-02-10,2010-09-17,"D","EX4 4FT",0
        // 24,"I",108495,10023119082,"1110L000165517","ENG",1,2010-09-18,,2016-02-10,2010-09-17,,"",,"","FLAT G.01 BLOCK G",,"",,"","BIRKS HALLS",14200580,"1","","",""
        // 28,"I",109427,10023119082,52172489,"","","FLAT G.01 BLOCK G","BIRKS HALL",,"","NEW NORTH ROAD","","","EXETER","EX4 4FT","S","1A","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
        OSDpa(uprn = 10023119082L,
          subBuildingName = "FLAT G.01 BLOCK G", buildingName = "BIRKS HALL", buildingNumber = "",
          dependentThoroughfareName = "",
          thoroughfareName = "NEW NORTH ROAD",
          doubleDependentLocality = "",
          dependentLocality = "",
          postTown = "EXETER",
          postcode = "EX4 4FT").normalise,
        Blpu(None, "EX4 4FT", '1', '2', 'E', 1110, "1.0,-1.0"),
        DbAddress("GB10023119082", List("Flat G.01 Block G, Birks Hall", "New North Road"), Some("Exeter"),
          "EX4 4FT", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, Some("1.0,-1.0")))

    )

    for (c <- cases) {
      val a = ExportDbAddress.exportDPA(c.dpa, c.blpu, "en")
      assert(a === c.expected)
    }
  }
}
