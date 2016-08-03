/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package ingest

import java.util.Date
import java.util.concurrent.SynchronousQueue

import addressbase.{OSBlpu, OSCsv, OSLpi}
import ingest.Ingester.{Blpu, Street}
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class SecondPassTest extends FunSuite with Matchers with MockitoSugar {

  // sample data here is in the new format
  OSCsv.setCsvFormat(2)

  // test data is long so disable scalastyle check
  // scalastyle:off

  class context {
    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val worker = new WorkQueue(status)
    val continuer = mock[Continuer]
    val lock = new SynchronousQueue[Boolean]()
    val model = new StateModel()
    val forwardData = ForwardData.chronicleInMemoryForUnitTest("DPA")
  }

  test(
    """Given an OS-LPI and a prior BLPU to match
       Then one record will be produced by processFile.
    """) {
    new context {
      val lpiData =
        """
          24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        """

      val csv = CsvParser.split(lpiData)

      forwardData.blpu.put(131041604L, Blpu("AB12 3CD", '1', 'E').pack)

      val out = new OutputWriter {
        var count = 0

        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(out: DbAddress) {
          assert(out.id === "GB131041604")
          assert(out.postcode === "AB12 3CD")
          count += 1
        }

        def end(completed: Boolean) = model
      }

      when(continuer.isBusy) thenReturn true

      val sp = new SecondPass(forwardData, continuer, Algorithm())
      worker.push("testing", {
        continuer =>
          lock.put(true)
          sp.processFile(csv, out)
      })

      lock.take()
      worker.awaitCompletion()
      assert(out.count === 1)
    }
  }


  test(
    """Given an OS-LPI and a prior BLPU to match,
       And the task is aborting
       Then no records will be produced by processFile.
    """) {
    new context {
      val lpiData =
        """
          24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        """

      val csv = CsvParser.split(lpiData)

      forwardData.blpu.put(131041604L, Blpu("AB12 3CD", '1', 'E').pack)

      val out = new OutputWriter {
        var count = 0

        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(out: DbAddress) {
          count += 1
        }

        def end(completed: Boolean) = model
      }

      when(continuer.isBusy) thenReturn false

      val sp = new SecondPass(forwardData, continuer, Algorithm())
      worker.push("testing", {
        continuer =>
          lock.put(true)
          sp.processFile(csv, out)
      })

      lock.take()
      worker.awaitCompletion()
      assert(out.count === 0)
    }
  }


  test(
    """Given an OS-LPI without a matching BLPU
       Then no records will be produced by processFile.
    """) {
    new context {

      val lpiData =
        """
          24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        """

      val csv = CsvParser.split(lpiData)

      forwardData.blpu.put(0L, Blpu("AB12 3CD", '1', 'E').pack)

      val out = new OutputWriter {
        var count = 0

        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(out: DbAddress) {
          count += 1
        }

        def end(completed: Boolean) = model
      }

      when(continuer.isBusy) thenReturn true

      val sp = new SecondPass(forwardData, continuer, Algorithm())
      worker.push("testing", {
        continuer =>
          lock.put(true)
          sp.processFile(csv, out)
      })

      lock.take()
      worker.awaitCompletion()
      assert(out.count === 0)
    }
  }


  test(
    """Given an OS-LPI and a prior BLPU to match
       Then the exported record will be correct.
    """) {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
      """

    val blpuData =
      """21,"I",801310,131041604,1,2,2008-07-28,,252508.00,654612.00,55.7623040,-4.3521941,1,9063,"S",2012-04-27,,2016-02-10,2007-04-27,"L","G77 6RT",0
      """


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus, osblpu.subdivision)

    val streetsMap = new java.util.HashMap[java.lang.Long, String]()
    streetsMap.put(48804683L, Street('A', "streetDescription", "locality-name", "town-name").pack)

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu.postcode, streetsMap, blpu.subdivision, Algorithm())
    assert(out.id === "GB131041604")
    assert(out.lines === List("Maidenhill Stables", "Locality-Name"))
    assert(out.town === Some("Town-Name"))
    assert(out.postcode === "G77 6RT")
    assert(out.subdivision === Some("GB-SCT"))
  }


  test(
    """Given an OS-LPI containing a house number as a range
       Then one record will be produced by exportLPI
       And the range will be formatted correctly.
    """) {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
      """

    val blpuData =
      """21,"I",801310,131041604,1,2,2008-07-28,,252508.00,654612.00,55.7623040,-4.3521941,1,9063,"S",2012-04-27,,2016-02-10,2007-04-27,"L","G77 6RT",0
      """


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus, osblpu.subdivision)

    val streetsMap = new java.util.HashMap[java.lang.Long, String]()
    streetsMap.put(48804683L, Street('A', "streetDescription", "locality name", "town-name").pack)

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu.postcode, streetsMap, blpu.subdivision, Algorithm())
    assert(out.id === "GB131041604")
    assert(out.lines === List("1a-2b Maidenhill Stables", "Locality Name"))
    assert(out.town === Some("Town-Name"))
    assert(out.postcode === "G77 6RT")
  }



  test(
    """Given an OS-LPI and a prior BLPU to match
       And there is a pre-existing street record,
       Then one record will be produced by exportLPI in which the street details are updated by the street record.
    """) {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL From STABLES",48804683,"1","","","Y"
      """

    val blpuData =
      """21,"I",801310,131041604,1,2,2008-07-28,,252508.00,654612.00,55.7623040,-4.3521941,1,9063,"S",2012-04-27,,2016-02-10,2007-04-27,"L","G77 6RT",0
      """


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus, osblpu.subdivision)

    val streetsMap = new java.util.HashMap[java.lang.Long, String]()
    streetsMap.put(48804683L, Street('A', "street From Description", "locality name", "town-name").pack)

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu.postcode, streetsMap, blpu.subdivision, Algorithm())
    assert(out.id === "GB131041604")
    assert(out.lines === List("Locality Name"))
    assert(out.town === Some("Town-Name"))
    assert(out.postcode === "G77 6RT")
    assert(out.subdivision === Some("GB-SCT"))
  }

  test(
    """Given an OS-LPI and a prior BLPU to match
       And there is an LPI record for the same uprn
       And there is a DPA record for the same uprn
       Then the DPA record will be output
    """) {
    val osHeader =
      """10,"NAG Hub - GeoPlace",9999,2016-02-19,0,2016-02-19,23:47:05,"2.0","F"
      """
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL From STABLES",48804683,"1","","","Y"
      """
    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,50.7337174,-3.4940473,1,9063,"E",2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
      """
    val dpaData =
      """28,"I",109437,131041604,50308610,"","","","39D",,"","POLSLOE ROAD","","","EXETER","EX1 2DN","S","1R","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      """


    val csvOSHeaderLine: Array[String] = CsvParser.split(osHeader).next()
    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()
    val csvDpaLine: Array[String] = CsvParser.split(dpaData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus, osblpu.subdivision)

    val boolTrue: Boolean = true
    val boolFalse: Boolean = false

    val fd = ForwardData.chronicleInMemoryForUnitTest("DPA")
    fd.blpu.put(131041604L, Blpu(blpu.postcode, blpu.logicalStatus, blpu.subdivision).pack)
    fd.uprns.add(131041604L)

    val continuer = mock[Continuer]

    when(continuer.isBusy) thenReturn boolTrue

    val secondPass = new SecondPass(fd, continuer, Algorithm())
    val iterator = Iterator(csvOSHeaderLine, csvLpiLine, csvBlpuLine, csvDpaLine)

    val outputWriter = mock[OutputWriter]

    secondPass.processFile(iterator, outputWriter)

    val argCap = ArgumentCaptor.forClass(classOf[DbAddress]);
    verify(outputWriter).output(argCap.capture())

    val dbAdd = argCap.getValue
    assert(dbAdd.id === "GB131041604")
    assert(dbAdd.town === Some("Exeter"))
    assert(dbAdd.postcode === "EX1 2DN")
  }

  test(
    """Given an OS-LPI and a prior BLPU to match
       And there is an LPI record for the same uprn
       And there is a second LPI record for the same uprn
       And there is are pre-existing street records,
       And there is no DPA record for the same uprn
       Then the first LPI record will be output
    """) {
    val osHeader =
      """10,"NAG Hub - GeoPlace",9999,2016-02-19,0,2016-02-19,23:47:05,"2.0","F"
      """
    val firstLpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","LPI ONE",48804683,"1","","","Y"
      """
    val secondLpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","LPI TWO",58804683,"1","","","Y"
      """
    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,50.7337174,-3.4940473,1,9063,"E",2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
      """


    val csvOSHeaderLine: Array[String] = CsvParser.split(osHeader).next()
    val csvFirstLpiLine: Array[String] = CsvParser.split(firstLpiData).next()
    val csvSecondLpiLine: Array[String] = CsvParser.split(secondLpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus, osblpu.subdivision)


    val boolTrue: Boolean = true

    val fd = ForwardData.chronicleInMemoryForUnitTest("DPA")
    fd.blpu.put(131041604L, Blpu(blpu.postcode, blpu.logicalStatus, blpu.subdivision).pack)
    fd.streets.put(48804683L, Street('A', "lpi-one", "lpi-locality-one", "lpi-town-one").pack)
    fd.streets.put(58804683L, Street('A', "lpi-two", "lpi-locality-two", "lpi-town-two").pack)

    val continuer = mock[Continuer]

    when(continuer.isBusy) thenReturn boolTrue

    val secondPass = new SecondPass(fd, continuer, Algorithm())
    val iterator = Iterator(csvOSHeaderLine, csvFirstLpiLine, csvSecondLpiLine, csvBlpuLine)

    val outputWriter = mock[OutputWriter]

    secondPass.processFile(iterator, outputWriter)

    val argCap = ArgumentCaptor.forClass(classOf[DbAddress]);
    verify(outputWriter).output(argCap.capture())

    val dbAdd = argCap.getValue
    assert(dbAdd.id === "GB131041604")
    assert(dbAdd.town === Some("Lpi-Town-One"))
    assert(dbAdd.postcode === "G77 6RT")
  }


  test("Given a BLPU with a country code 'eg.E', a matching subdivision should be returned 'eg.GB-ENG'.") {
    aTest('E', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-ENG"), "Invalid country should be 'England'")
    })
    aTest('S', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-SCT"), "Invalid country should be 'Scotland'")
    })
    aTest('W', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-WLS"), "Invalid country should be 'Wales'")
    })
    aTest('N', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-NIR"), "Invalid country should be 'Northern Ireland'")
    })
    aTest('L', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-CHA"), "Invalid country should be 'Channel Islands'")
    })
    aTest('M', (count:Int, addr:DbAddress) => {
      assert(count === 1)
      assert(addr.subdivision === Some("GB-IOM"), "Invalid country should be 'Isle of Man'")
    })

  }

  test("Given a BLPU with a country code of unknown(J), nothing is returned.") {
      aTest('J', (count:Int, addr:DbAddress) => {
        assert(count === 0)
      })
  }

  def aTest( countryCode:Char, f: (Int, DbAddress) => Unit) {
    new context {

      val lpiData ="""24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        """

      val csv = CsvParser.split(lpiData)

      forwardData.blpu.put(131041604L, Blpu("AB12 3CD", '1', countryCode).pack)

      val out = new OutputWriter {
        var count = 0
        var addr: DbAddress = _

        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(out: DbAddress) {
          addr = out
          count += 1
        }

        def end(completed: Boolean) = model
      }

      when(continuer.isBusy) thenReturn true

      val sp = new SecondPass(forwardData, continuer, Algorithm())
      worker.push("testing", {
        continuer =>
          lock.put(true)
          sp.processFile(csv, out)
      })

      lock.take()
      worker.awaitCompletion()
      f(out.count, out.addr)
//      assert(out.count === 0)
    }
  }
}
