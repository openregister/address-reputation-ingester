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

import java.io.{File, StringReader}

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import uk.co.bigbeeconsultants.http.util.DiagnosticTimer
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SecondPassTest extends FunSuite with Matchers {

  // sample data here is in the old format
  OSCsv.setCsvFormat(1)

  test("can find a LPI item can find a BLPU item") {
    val lpiData =
      """
        |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(lpiData))

    val blpuMap = mutable.HashMap.empty[Long, Blpu] + (131041604L -> Blpu("AB12 3CD", '1'))
    val streetsMap = mutable.HashMap.empty[Long, Street]
    val fd = ForwardData(blpuMap, new mutable.HashSet(), streetsMap)
    val out = (out: DbAddress) => {
      assert(out.id === "GB131041604")
//      assert(out.lines === List("Maidenhill Stables", "xxx")) don't care here
//      assert(out.town === "townName") don't care here
      assert(out.postcode === "AB12 3CD")
    }

    SecondPass.processLine(csv, fd, out)
  }


  test("can find a LPI item can NOT find a blpu item") {
    val lpiData =
      """
        |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val csv = CsvParser.split(lpiData)

    val blpuMap = mutable.HashMap.empty[Long, Blpu] + (0L -> Blpu("AB12 3CD", '1'))
    val streetsMap = mutable.HashMap.empty[Long, Street]
    val fd = ForwardData(blpuMap, new mutable.HashSet(), streetsMap)
    val out = (out: DbAddress) => {
      fail()
    }

    SecondPass.processLine(csv, fd, out)
  }


  test("Check the exported format of a LPI message ") {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
    // 0   1  2      3         4 5 6         7 8         9        10 11   12          14         15         16  17        18
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val out = (out: DbAddress) => {
      assert(out.id === "GB131041604")
      assert(out.lines === List("Maidenhill Stables", "Locality-Name"))
      assert(out.town === "Town-Name")
      assert(out.postcode === "G77 6RT")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "locality-name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }


  test("LPI number range") {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val out = (out: DbAddress) => {
      assert(out.id === "GB131041604")
      assert(out.lines === List("1a-2b Maidenhill Stables", "Locality Name"))
      assert(out.town === "Town-Name")
      assert(out.postcode === "G77 6RT")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "locality name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }



  test("check LPI with a street name that needs to be removed ") {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL From STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val out = (out: DbAddress) => {
      assert(out.id === "GB131041604")
      assert(out.lines === List("Locality Name"))
      assert(out.town === "Town-Name")
      assert(out.postcode === "G77 6RT")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "street From Description", "locality name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }


  test("check 2nd pass") {
    val sample = new File(getClass.getClassLoader.getResource("SX9090-first20.zip").getFile)
    val fd = ForwardData.empty
    val dummyOut = (out: DbAddress) => {}

    SecondPass.secondPass(Vector(sample), fd, dummyOut, new DiagnosticTimer)
  }


  test("check 2nd with invalid csv will generate an error") {
    val sample = new File(getClass.getClassLoader.getResource("invalid24.zip").getFile)
    val fd = ForwardData.empty
    val dummyOut = (out: DbAddress) => {}

    val e = intercept[Exception] {
      SecondPass.secondPass(Vector(sample), fd, dummyOut, new DiagnosticTimer)
    }

    assert(e.getMessage === "3")
  }


  test("check 2nd with nonexistent csv will generate an error") {
    val fd = ForwardData.empty
    val dummyOut = (out: DbAddress) => {}

    val e = intercept[Exception] {
      SecondPass.secondPass(Vector(new File("nonexistent.zip")), fd, dummyOut, new DiagnosticTimer)
    }
    assert(e.getMessage.contains("No such file or directory"))
    assert(e.getMessage.contains("nonexistent.zip"))
  }
}
