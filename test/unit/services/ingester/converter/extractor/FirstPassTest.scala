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

package services.ingester.converter.extractor

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import services.ingester.exec.Task
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class FirstPassTest extends FunSuite with Matchers {

  // sample data here is in the old format
  OSCsv.setCsvFormat(1)

  val dummyOut = (out: DbAddress) => {}

  // test data is long so disable scalastyle check
  // scalastyle:off

  class context(data: String) {
    val csv = CsvParser.split(data)
    val logger = new StubLogger
    val task = new Task(logger)
  }

  test(
    """Given an OS-StreetDescriptor
       the street table will be augmented correctly
    """) {
    new context(
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""
    ) {

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.head === 48504236 -> Street('A', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPA UPRNs, 1 streets")
    }
  }

  test(
    """Given an OS-Street and OS-StreetDescriptor including both English and Welsh
       the street table will be augmented correctly
       and the Welsh part will be ignored
    """) {
    new context(
      """
        |11,"I",912885,47208194,2,6825,,,,,0,2008-03-06,,2008-03-06,2004-01-28,237846.00,233160.00,237363.00,229392.00,10
        |15,"I",912886,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CAERFYRDDIN","SIR GAR","CYM"
        |15,"I",912887,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CARMARTHEN","CARMARTHENSHIRE","ENG"
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.head === 47208194 -> Street('2', "CWMDUAD TO CYNWYL ELFED", "CWMDUAD", "CARMARTHEN"))
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPA UPRNs, 1 streets")
    }
  }

  test(
    """Given an OS-Street and OS-StreetDescriptor
       the street table will be augmented correctly
    """) {
    new context(
      """
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.head === 48504236 -> Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPA UPRNs, 1 streets")
    }
  }

  test(
    """Given an OS-Street and OS-StreetDescriptor in reverse order
       the street table will be augmented correctly
    """) {
    new context(
      """
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.head === 48504236 -> Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPA UPRNs, 1 streets")
    }
  }


  test(
    """Given an OS-BLPU
       the BLPU table will be augmented correctly
    """) {
    new context(
      """
        |10,"GeoPlace",9999,2011-07-08,1,2011-07-08,16:00:30,"1.0","F"
        |21,"I",521480,320077134,1,2,2011-09-09,,354661.00,702526.00,1,9066,1992-06-10,,2004-08-10,2004-08-09,"S","KY10 2PY",0
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.blpuTable.size === 1)
      assert(firstPass.blpuTable.head === 320077134 -> Blpu("KY10 2PY", '1'))
      assert(firstPass.sizeInfo === "First pass obtained 1 BLPUs, 0 DPA UPRNs, 0 streets")
    }
  }


  test(
    """Given an OS-DPA
       the DPA table will be augmented correctly
       and one DPA record will be produced
    """) {
    new context(
      """
        |28,"I",950823,9051119283,9051309667,35342,"","","","1 UPPER KENNERTY MILL COTTAGES",,"","","","","PETERCULTER","AB14 0LQ","S","","","","","","",2015-05-18,2003-02-03,,2011-03-16,2003-02-03
        | """.stripMargin
    ) {

      val out = (out: DbAddress) => {
        assert(out.id === "GB9051119283")
        assert(out.lines === List("1 Upper Kennerty Mill Cottages"))
        assert(out.town === "Peterculter")
        assert(out.postcode === "AB14 0LQ")
      }

      val firstPass = new FirstPass(dummyOut, task)
      firstPass.processFile(csv, out)

      assert(firstPass.dpaTable.size === 1)
      assert(firstPass.dpaTable.head === 9051119283L)
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 1 DPA UPRNs, 0 streets")
    }
  }
}
