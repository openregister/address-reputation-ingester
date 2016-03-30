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

import java.io.{ByteArrayInputStream, File, InputStream, StringReader}
import java.util.Collections

import org.apache.commons.compress.archivers.zip.{ZipArchiveEntry, ZipFile}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import services.addressimporter.converter.Extractor.{Blpu, Street}
import services.addressimporter.converter.extractor.{FirstPass, SecondPass}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser

import scala.collection.immutable.{HashMap, HashSet}
import scala.util.Success

class ExtractorSuite extends FunSuite with Matchers with MockitoSugar {

  implicit val dummyOut = (out: DbAddress) => {}

  // test data is long so disabe scalastyle check
  // scalastyle:off  line.size.limit

  test("Can find OSStreetDescriptor details") {
    val data =
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.streets.size === 1)
    assert(result.streets.headOption === Some((48504236, Street('A', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
  }


  test("Can find StreetDescriptor and Street details and ignore Welsh") {
    val data =
      """
        |11,"I",912885,47208194,2,6825,,,,,0,2008-03-06,,2008-03-06,2004-01-28,237846.00,233160.00,237363.00,229392.00,10
        |15,"I",912886,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CAERFYRDDIN","SIR GAR","CYM"
        |15,"I",912887,47208194,"CWMDUAD TO CYNWYL ELFED","CWMDUAD","CARMARTHEN","CARMARTHENSHIRE","ENG"
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.streets.size === 1)
    assert(result.streets.headOption === Some((47208194, Street('2', "CWMDUAD TO CYNWYL ELFED", "CWMDUAD", "CARMARTHEN"))))
  }

  test("Can find StreetDescriptor and Street details") {
    val data =
      """
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.streets.size === 1)
    assert(result.streets.headOption === Some((48504236, Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
  }

  test("Can find Street and StreetDescriptor details") {
    val data =
      """
        |15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG"
        |11,"I",31067,48504236,2,9060,4,2015-01-14,,,0,2014-11-20,2015-01-14,2015-01-14,2014-11-20,261812.01,613893.54,261808.05,613853.62,999
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.streets.size === 1)
    assert(result.streets.headOption === Some((48504236, Street('2', "A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE", "", "NEW CUMNOCK"))))
  }


  test("can find a BLPU item") {
    val data =
      """
        |10,"GeoPlace",9999,2011-07-08,1,2011-07-08,16:00:30,"1.0","F"
        |21,"I",521480,320077134,1,2,2011-09-09,,354661.00,702526.00,1,9066,1992-06-10,,2004-08-10,2004-08-09,"S","KY10 2PY",0
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.blpu.size === 1)
    assert(result.blpu.headOption === Some((320077134, Blpu("KY10 2PY", '1'))))
  }


  test("can find a DPA item") {
    val data =
      """
        |28,"I",950823,9051119283,9051309667,35342,"","","","1 UPPER KENNERTY MILL COTTAGES",,"","","","","PETERCULTER","AB14 0LQ","S","","","","","","",2015-05-18,2003-02-03,,2011-03-16,2003-02-03
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val streetsMap = HashMap.empty[Long, Street]
    val lpiLogicStatusMap = HashMap.empty[Long, Byte]

    val result = FirstPass.processFile(csv, streetsMap, lpiLogicStatusMap, dummyOut)

    assert(result.dpa.size === 1)
    val expectedId = 9051119283L
    assert(result.dpa.headOption === Some(expectedId))
  }


  test("can find a LPI item can find a blpu item") {
    val data =
      """
        |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val blpuMap = HashMap.empty[Long, Blpu] + (131041604L -> Blpu("AB12 3CD", '1'))
    val streetsMap = HashMap.empty[Long, Street]

    val result: HashMap[Long, Blpu] = SecondPass.processLine(csv, blpuMap, streetsMap, dummyOut)

    assert(result.size === 0)
  }


  test("can find a LPI item can NOT find a blpu item") {
    val data =
      """
        |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val csv = CsvParser.split(new StringReader(data))

    val blpuMap = HashMap.empty[Long, Blpu] + (0L -> Blpu("AB12 3CD", '1'))
    val streetsMap = HashMap.empty[Long, Street]

    val result: HashMap[Long, Blpu] = SecondPass.processLine(csv, blpuMap, streetsMap, dummyOut)

    assert(result.size === 1, "item is not removed")
  }


  test("Check the exported format of a DLP message ") {
    val data =
      """28,"I",950823,9051119283,9051309667,35342,"","","","1 UPPER KENNERTY MILL COTTAGES",,"","","","","PETERCULTER","AB14 0LQ","S","","","","","","",2015-05-18,2003-02-03,,2011-03-16,2003-02-03 """.stripMargin

    val csvLine: Array[String] = CsvParser.split(new StringReader(data)).next()

    val out = (out: DbAddress) => {
      assert(out.uprn === "9051119283", "uprn")
      assert(out.line1 === "1 UPPER KENNERTY MILL COTTAGES", "Line1")
      assert(out.line2 === "", "Line2")
      assert(out.line3 === "", "Line3")
      assert(out.town === "PETERCULTER", "Town")
      assert(out.postcode === "AB14 0LQ", "Postcode")
    }

    val dpa = OSDpa(csvLine)
    FirstPass.exportDPA(dpa)(out)
  }


  test("Check the exported format of a LPI message ") {
    val data =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLine: Array[String] = CsvParser.split(new StringReader(data)).next()

    val csvBlpuLine: Array[String] = CsvParser.split(new StringReader(blpuData)).next()


    val out = (out: DbAddress) => {
      assert(out.uprn === "131041604", "uprn")
      assert(out.line1 === "MAIDENHILL STABLES", "Line1")
      assert(out.line2 === "", "Line2")
      assert(out.line3 === "localityName", "Line3")
      assert(out.town === "townName", "Town")
      assert(out.postcode === "G77 6RT", "Postcode")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "localityName", "townName"))

    val lpi = OSLpi(csvLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }


  test("LPI number range") {
    val data =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLine: Array[String] = CsvParser.split(new StringReader(data)).next()

    val csvBlpuLine: Array[String] = CsvParser.split(new StringReader(blpuData)).next()


    val out = (out: DbAddress) => {
      assert(out.uprn === "131041604", "uprn")
      assert(out.line1 === "1a-2b MAIDENHILL STABLES", "Line1")
      assert(out.line2 === "", "Line2")
      assert(out.line3 === "localityName", "Line3")
      assert(out.town === "townName", "Town")
      assert(out.postcode === "G77 6RT", "Postcode")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "localityName", "townName"))

    val lpi = OSLpi(csvLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }



  test("check LPI with a street name that needs to be removed ") {
    val data =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL From STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLine: Array[String] = CsvParser.split(new StringReader(data)).next()

    val csvBlpuLine: Array[String] = CsvParser.split(new StringReader(blpuData)).next()


    val out = (out: DbAddress) => {
      assert(out.uprn === "131041604", "uprn")
      assert(out.line1 === "", "Line1")
      assert(out.line2 === "", "Line2")
      assert(out.line3 === "localityName", "Line3")
      assert(out.town === "townName", "Town")
      assert(out.postcode === "G77 6RT", "Postcode")
    }

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = HashMap[Long, Street](48804683L -> Street('A', "street From Description", "localityName", "townName"))

    val lpi = OSLpi(csvLine)
    SecondPass.exportLPI(lpi, blpu, streetsMap)(out)
  }


  test("string processing functions ") {
    import OSCleanup._

    assert("  ".cleanup === "")
    assert("\"\"".cleanup === "")
    assert("\"Hello\"".cleanup === "Hello")
  }


  test("Check the combining of two empty ForwardData objects works") {
    val fd1 = ForwardData.empty
    val fd2 = ForwardData.empty

    val expectedResult = ForwardData.empty

    val result = fd1.update(fd2)

    assert(result === expectedResult)
  }


  test("Check the combining of two ForwardData objects works") {
    val fd1 = ForwardData.empty

    val blpu1 = 1L -> Blpu("", 'a')

    val fd2 = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](blpu1))

    val result = fd1.update(fd2)

    val expectedResult = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](blpu1))

    assert(result === expectedResult)
  }


  test("Check the combining two ForwardData will remove item from dpa forward references") {
    val fd1 = ForwardData.empty.copy(dpa = HashSet[Long](1L))

    val fd2 = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](1L -> Blpu("", 'a')))

    val result = fd1.update(fd2)

    val expectedResult = ForwardData.empty

    assert(result === expectedResult)
  }


  test("zipreader test") {
    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()
    entries.add(new ZipArchiveEntry(""))

    val inputStream: InputStream = new ByteArrayInputStream("World".getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream


    val result = LoadZip.zipReader(mockZipFile) { itr =>
    }

    assert(result.isSuccess)
  }


  test("zipreader test empty zipfile") {
    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()

    val inputStream: InputStream = new ByteArrayInputStream("World".getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream


    val result = LoadZip.zipReader(mockZipFile) { itr =>
    }

    assert(result.isFailure)
    result.failed.get shouldBe a[EmptyFileException]
  }


  test("check 1st pass") {
    val data =
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""

    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()
    entries.add(new ZipArchiveEntry(""))

    val inputStream: InputStream = new ByteArrayInputStream(data.getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream


    val result = FirstPass.firstPass(Vector(mockZipFile), dummyOut)

    assert(result.isSuccess)
  }


  test("check 1st pass with invalid csv will generate an error") {
    val data =
      """15,"I",31068,48504236"""

    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()
    entries.add(new ZipArchiveEntry(""))

    val inputStream: InputStream = new ByteArrayInputStream(data.getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream


    val result = FirstPass.firstPass(Vector(mockZipFile), dummyOut)

    assert(result.isFailure)
    assert(result.failed.get.getMessage === "8")
  }


  test("check 2nd pass") {
    val data =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()
    entries.add(new ZipArchiveEntry(""))

    val inputStream: InputStream = new ByteArrayInputStream(data.getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream

    val fd = ForwardData.empty

    val result = SecondPass.secondPass(Vector(mockZipFile), fd, dummyOut)

    assert(result.isSuccess)
  }


  test("check 2nd with invalid csv will generate an error") {
    val data =
      """24,"I",913236
        | """.stripMargin

    val mockZipFile = mock[ZipFile]

    val entries: java.util.List[ZipArchiveEntry] = new java.util.ArrayList[ZipArchiveEntry]()
    entries.add(new ZipArchiveEntry(""))

    val inputStream: InputStream = new ByteArrayInputStream(data.getBytes)

    when(mockZipFile.getEntries) thenReturn Collections.enumeration(entries)
    when(mockZipFile.getInputStream(any[ZipArchiveEntry])) thenReturn inputStream

    val fd = ForwardData.empty

    val result = SecondPass.secondPass(Vector(mockZipFile), fd, dummyOut)

    assert(result.isFailure)
    assert(result.failed.get.getMessage === "3")
  }


  test("all things ForwardData") {
    val fd1 = ForwardData.empty

    assert(fd1.toString === "ForwardData(Map(),Set(),Map(),Map())")
  }


  test("Having no files should successfully return nothing") {
    val mockFile = mock[File]

    when(mockFile.isDirectory) thenReturn true
    when(mockFile.listFiles) thenReturn Array.empty[File]

    val result = Extractor.extract(mockFile, dummyOut)

    assert(result === Some(Success(0)))
  }


}
