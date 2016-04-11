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

import java.io.File
import java.util.concurrent.ArrayBlockingQueue

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import services.ingester.exec.Task
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class FirstPassTest extends FunSuite with Matchers {

  // sample data here is in the old format
  OSCsv.setCsvFormat(1)

  val dummyOut = (out: DbAddress) => {}

  // test data is long so disabe scalastyle check
  // scalastyle:off  line.size.limit

  class context(data: String) {
    val csv = CsvParser.split(data)
    val logger = new StubLogger
    val task = new Task(logger)
  }

  test("Can find OSStreetDescriptor details") {
    new context(
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.headOption === Some((48504236, Street('A', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
    }
  }


  test("Can find StreetDescriptor and Street details and ignore Welsh") {
    new context(
      """
        |11,"I",912885,47208194,2,6825,,,,,0,2008-03-06,,2008-03-06,2004-01-28,237846.00,233160.00,237363.00,229392.00,10
        |15,"I",912886,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CAERFYRDDIN","SIR GAR","CYM"
        |15,"I",912887,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CARMARTHEN","CARMARTHENSHIRE","ENG"
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.headOption === Some((47208194, Street('2', "CWMDUAD TO CYNWYL ELFED", "CWMDUAD", "CARMARTHEN"))))
    }
  }

  test("Can find StreetDescriptor and Street details") {
    new context(
      """
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.headOption === Some((48504236, Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
    }
  }

  test("Can find Street and StreetDescriptor details") {
    new context(
      """
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.streetTable.size === 1)
      assert(firstPass.streetTable.headOption === Some((48504236, Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
    }
  }


  test("can find a BLPU item") {
    new context(
      """
        |10,"GeoPlace",9999,2011-07-08,1,2011-07-08,16:00:30,"1.0","F"
        |21,"I",521480,320077134,1,2,2011-09-09,,354661.00,702526.00,1,9066,1992-06-10,,2004-08-10,2004-08-09,"S","KY10 2PY",0
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.blpuTable.size === 1)
      assert(firstPass.blpuTable.headOption === Some((320077134, Blpu("KY10 2PY", '1'))))
    }
  }


  test("can find a DPA item") {
    new context(
      """
        |28,"I",950823,9051119283,9051309667,35342,"","","","1 UPPER KENNERTY MILL COTTAGES",,"","","","","PETERCULTER","AB14 0LQ","S","","","","","","",2015-05-18,2003-02-03,,2011-03-16,2003-02-03
        | """.stripMargin
    ) {

      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.processFile(csv, dummyOut)

      assert(firstPass.dpaTable.size === 1)
      val expectedId = 9051119283L
      assert(firstPass.dpaTable.headOption === Some(expectedId))
    }
  }


  test("Check the exported format of a DLP message ") {
    new context(
      """28,"I",950823,9051119283,9051309667,35342,"","","","1 UPPER KENNERTY MILL COTTAGES",,"","","","","PETERCULTER","AB14 0LQ","S","","","","","","",2015-05-18,2003-02-03,,2011-03-16,2003-02-03 """.stripMargin
    ) {

      val csvLine: Array[String] = csv.next()

      val out = (out: DbAddress) => {
        assert(out.id === "GB9051119283")
        assert(out.lines === List("1 Upper Kennerty Mill Cottages"))
        assert(out.town === "Peterculter")
        assert(out.postcode === "AB14 0LQ")
      }

      val dpa = OSDpa(csvLine)
      val firstPass = new FirstPass(Nil, dummyOut, task, new DiagnosticTimer)
      firstPass.exportDPA(dpa)(out)
    }
  }


  test("check 1st pass") {
    val sample = new File(getClass.getClassLoader.getResource("SX9090-first20.zip").getFile)
    val logger = new StubLogger
    val task = new Task(logger)
    val firstPass = new FirstPass(List(sample), dummyOut, task, new DiagnosticTimer).firstPass
  }


  test("check 1st pass with invalid csv will generate an error") {
    val sample = new File(getClass.getClassLoader.getResource("invalid15.zip").getFile)
    val logger = new StubLogger
    val task = new Task(logger)
    val stuff = new ArrayBlockingQueue[Boolean](1)
    task.start {
      stuff.take() // blocks until signalled
    }
    val e = intercept[Exception] {
      new FirstPass(List(sample), dummyOut, task, new DiagnosticTimer).firstPass
    }
    assert(e.getMessage === "8")
    stuff.offer(true) // release the lock
  }

}
