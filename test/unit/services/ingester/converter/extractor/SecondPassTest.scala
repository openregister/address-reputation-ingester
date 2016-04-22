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

import java.util.concurrent.SynchronousQueue

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import services.ingester.converter.Extractor.{Blpu, Street}
import services.ingester.converter._
import services.ingester.exec.{Continuer, WorkQueue}
import services.ingester.writers.OutputWriter
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class SecondPassTest extends FunSuite with Matchers with MockitoSugar {

  // sample data here is in the old format
  OSCsv.setCsvFormat(1)

  // test data is long so disable scalastyle check
  // scalastyle:off

  class context {
    val logger = new StubLogger
    val worker = new WorkQueue(logger)
    val continuer = mock[Continuer]
    val lock = new SynchronousQueue[Boolean]()
  }

  test(
    """Given an OS-LPI and a prior BLPU to match
       Then one record will be produced by processFile.
    """) {
    new context {
      val lpiData =
        """
          |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
          | """.stripMargin

      val csv = CsvParser.split(lpiData)

      val blpuMap = mutable.HashMap.empty[Long, Blpu] + (131041604L -> Blpu("AB12 3CD", '1'))
      val streetsMap = mutable.HashMap.empty[Long, Street]
      val fd = ForwardData(blpuMap, new mutable.HashSet(), streetsMap)

      val out = new OutputWriter {
        var count = 0

        def close() {}

        def output(out: DbAddress) {
          assert(out.id === "GB131041604")
          assert(out.postcode === "AB12 3CD")
          count += 1
        }
      }

      when(continuer.isBusy) thenReturn true

      val sp = new SecondPass(fd, continuer)
      worker.push("testing", {
        lock.offer(true)
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
          |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
          | """.stripMargin

      val csv = CsvParser.split(lpiData)

      val blpuMap = mutable.HashMap.empty[Long, Blpu] + (131041604L -> Blpu("AB12 3CD", '1'))
      val streetsMap = mutable.HashMap.empty[Long, Street]
      val fd = ForwardData(blpuMap, new mutable.HashSet(), streetsMap)

      val out = new OutputWriter {
        var count = 0

        def close() {}

        def output(out: DbAddress) {
          count += 1
        }
      }

      when(continuer.isBusy) thenReturn false

      val sp = new SecondPass(fd, continuer)
      worker.push("testing", {
        lock.offer(true)
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
          |24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,,"",,"","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
          | """.stripMargin

      val csv = CsvParser.split(lpiData)

      val blpuMap = mutable.HashMap.empty[Long, Blpu] + (0L -> Blpu("AB12 3CD", '1'))
      val streetsMap = mutable.HashMap.empty[Long, Street]
      val fd = ForwardData(blpuMap, new mutable.HashSet(), streetsMap)
      val out = new OutputWriter {
        var count = 0

        def close() {}

        def output(out: DbAddress) {
          count += 1
        }
      }

      when(continuer.isBusy) thenReturn true

      val sp = new SecondPass(fd, continuer)
      worker.push("testing", {
        lock.offer(true)
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
        | """.stripMargin

    val blpuData =
    // 0   1  2      3         4 5 6         7 8         9        10 11   12          14         15         16  17        18
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "locality-name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu, streetsMap)
    assert(out.id === "GB131041604")
    assert(out.lines === List("Maidenhill Stables", "Locality-Name"))
    assert(out.town === "Town-Name")
    assert(out.postcode === "G77 6RT")
  }


  test(
    """Given an OS-LPI containing a house number as a range
       Then one record will be produced by exportLPI
       And the range will be formatted correctly.
    """) {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "streetDescription", "locality name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu, streetsMap)
    assert(out.id === "GB131041604")
    assert(out.lines === List("1a-2b Maidenhill Stables", "Locality Name"))
    assert(out.town === "Town-Name")
    assert(out.postcode === "G77 6RT")
  }



  test(
    """Given an OS-LPI and a prior BLPU to match
       And there is a pre-existing street record,
       Then one record will be produced by exportLPI in which the street details are updated by the street record.
    """) {
    val lpiData =
      """24,"I",913236,131041604,"9063L000011164","ENG",1,2007-04-27,,2008-07-22,2007-04-27,1,"a",2,"b","",,"",,"","MAIDENHILL From STABLES",48804683,"1","","","Y"
        | """.stripMargin

    val blpuData =
      """21,"I",913235,131041604,1,2,2008-07-28,,252508.00,654612.00,1,9063,2007-04-27,,2009-09-03,2007-04-27,"S","G77 6RT",0
        | """.stripMargin


    val csvLpiLine: Array[String] = CsvParser.split(lpiData).next()
    val csvBlpuLine: Array[String] = CsvParser.split(blpuData).next()

    val osblpu = OSBlpu(csvBlpuLine)
    val blpu = Blpu(osblpu.postcode, osblpu.logicalStatus)

    val streetsMap = mutable.HashMap[Long, Street](48804683L -> Street('A', "street From Description", "locality name", "town-name"))

    val lpi = OSLpi(csvLpiLine)
    val out = ExportDbAddress.exportLPI(lpi, blpu, streetsMap)
    assert(out.id === "GB131041604")
    assert(out.lines === List("Locality Name"))
    assert(out.town === "Town-Name")
    assert(out.postcode === "G77 6RT")
  }

}
