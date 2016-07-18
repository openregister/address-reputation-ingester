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

import java.io.File
import java.util.Date
import java.util.concurrent.SynchronousQueue

import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.exec.WorkQueue
import services.model.{StateModel, StatusLogger}
import ingest.writers.OutputWriter
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.logging.StubLogger

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class IngesterTest extends FunSuite with MockitoSugar {

  // scalastyle:off
  class context {
    val logger = new StubLogger
    val model = new StateModel()
    val status = new StatusLogger(logger)
    val worker = new WorkQueue(status)
    val lock = new SynchronousQueue[Boolean]()
  }

  test("finding files recursively: results should be sorted and include subdirectories") {
    val dir = new File(getClass.getClassLoader.getResource(".").getFile)
    val found = Ingester.listFiles(dir, ".zip")
    assert(found.head.getName === "3files.zip")
    assert(found.last.getPath.endsWith("/exeter/1/sample/SX9090-first3600.zip"))
  }


  test("at the end of the process, ForwardData is closed") {
    new context {
      val mockFile = mock[File]
      val dummyOut = mock[OutputWriter]
      val fd = mock[ForwardData]

      when(mockFile.isDirectory) thenReturn true
      when(mockFile.listFiles) thenReturn Array.empty[File]
      when(dummyOut.existingTargetThatIsNewerThan(any[Date])) thenReturn None

      worker.push("testing", {
        continuer =>
          new Ingester(continuer, model, status, fd).ingest(mockFile, dummyOut)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()
      verify(fd).close()
    }
  }


  test("Having no files should not throw any exception") {
    new context {
      val mockFile = mock[File]
      val dummyOut = mock[OutputWriter]

      when(mockFile.isDirectory) thenReturn true
      when(mockFile.listFiles) thenReturn Array.empty[File]
      when(dummyOut.existingTargetThatIsNewerThan(any[Date])) thenReturn None

      worker.push("testing", {
        continuer =>
          new Ingester(continuer, model, status, ForwardData.chronicleInMemoryForUnitTest()).ingest(mockFile, dummyOut)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()
    }
  }


  test("Having some old files and an up-to-date target should cause the ingestion to be skipped") {
    new context {
      val mockFile = mock[File]
      val dummyOut = mock[OutputWriter]

      when(mockFile.isDirectory) thenReturn true
      when(mockFile.listFiles) thenReturn List(new File("foo.zip")).toArray
      when(dummyOut.existingTargetThatIsNewerThan(any[Date])) thenReturn Some("foo")
      var result = false

      worker.push("testing", {
        continuer =>
          result = new Ingester(continuer, model, status, ForwardData.chronicleInMemoryForUnitTest()).ingest(mockFile, dummyOut)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()
      assert(result === true)
    }
  }


  test(
    """given a zip archive containing one file,
       Ingester should iterate over the CSV lines it contains
    """) {
    new context {
      val sample = new File(getClass.getClassLoader.getResource("exeter/1/sample/SX9090-first3600.zip").getFile)
      val addressesProduced = new mutable.ListBuffer[DbAddress]()
      var closed = false

      val out = new OutputWriter {
        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(a: DbAddress) {
          addressesProduced += a
        }

        def end(completed: Boolean) = {
          closed = true
          model
        }
      }

      worker.push("testing", {
        continuer =>
          new Ingester(continuer, model, status, ForwardData.simpleHeapInstance()).ingest(List(sample), out)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()

      assert(logger.infos.map(_.message) === List(
        "Info:Starting testing.",
        "Info:Starting first pass through 1 files.",
        "Info:Reading zip entry SX9090-first3600.csv...",
        "Info:Reading from 1 CSV files in {} took {}.",
        "Info:First pass obtained 30 BLPUs, 25 DPAs, 1683 streets.",
        "Info:First pass complete after {}.",
        "Info:Starting second pass through 1 files.",
        "Info:Reading zip entry SX9090-first3600.csv...",
        "Info:Reading from 1 CSV files in {} took {}.",
        "Info:Second pass processed 25 DPAs, 4 LPIs.",
        "Info:Ingester finished after {}.",
        "Info:Finished testing after {}."
      ))
      assert(addressesProduced.size === 29)
      assert(!closed) // the writer is closed at a higher scope level

      // 11,"I",211,14200711,1,1110,2,1990-01-01,1,8,0,2003-07-03,,2007-07-19,2003-07-03,293056.00,091088.00,292978.00,091480.00,10
      // 15,"I",212,14200711,"RIVERMEAD ROAD","","EXETER","DEVON","ENG"
      // 21,"I",3385,10023117083,1,2,2008-03-26,,292928.15,091203.52,1,1110,2008-03-26,,2012-04-03,2008-03-26,"S","EX2 4RL",0
      // 23,"I",3386,10023117083,"1110X014144892","30982182",,"7666VC",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3387,10023117083,"1110X600059475","osgb4000000025305695",5,"7666MI",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3388,10023117083,"1110X700059475","osgb1000012316391",3,"7666MT",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3389,10023117083,"1110X900059475","osgb1000002274376972",4,"7666MA",2008-03-26,,2012-04-03,2008-03-26
      // 24,"I",3390,10023117083,"1110L000163496","ENG",1,2008-03-26,,2008-03-26,2008-03-26,,"",,"","",78,"",,"","",14200711,"1","","","Y"
      // 28,"I",3391,10023117083,,8754179,"","","","",78,"","RIVERMEAD ROAD","","","EXETER","EX2 4RL","S","","","","","","",2014-04-22,2008-03-26,,2012-04-03,2008-03-26
      // 32,"I",3392,10023117083,"1110C000059475","RD","AddressBase Premium Classification Scheme",1.0,2008-03-26,,2012-04-03,2008-03-26
      assert(addressesProduced(0) === DbAddress("GB10023117083", List("78 Rivermead Road"), "Exeter", "EX2 4RL", "GB-ENG"))

      // 11,"I",1073,14200493,1,1110,2,1990-01-01,1,8,0,1995-06-20,,2007-07-19,1995-06-20,292385.00,093282.00,292285.00,092937.00,10
      // 15,"I",1074,14200493,"LONGBROOK STREET","","EXETER","DEVON","ENG"
      // 21,"I",3409,10023117221,1,2,2008-07-04,100041045306,292339.27,093057.05,1,1110,2008-07-04,,2012-04-03,2008-07-04,"S","EX4 6AL",0
      // 23,"I",3410,10023117221,"1110X014242428","6896998000",,"7666VC",2008-07-04,,2012-04-03,2008-07-04
      // 23,"I",3411,10023117221,"1110X600059467","osgb4000000025336209",3,"7666MI",2008-07-04,,2012-04-03,2008-07-04
      // 23,"I",3412,10023117221,"1110X900059467","osgb1000002274485088",3,"7666MA",2008-07-04,,2012-04-03,2008-07-04
      // 24,"I",3413,10023117221,"1110L000163644","ENG",1,2008-07-04,,2010-07-28,2008-07-04,10,"",,"","",58,"",64,"","ISCA LOFTS",14200493,"1","","","Y"
      // 28,"I",3414,10023117221,100041045306,51232954,"","","10","ISCA LOFTS  58-64",,"","LONGBROOK STREET","","","EXETER","EX4 6AL","S","","","","","","",2014-04-22,2008-07-04,,2012-04-03,2008-07-04
      assert(addressesProduced(3) === DbAddress("GB10023117221", List("10 Isca Lofts 58-64", "Longbrook Street"), "Exeter", "EX4 6AL", "GB-ENG")) //TODO This is not really correct

      // 11,"I",211,14200711,1,1110,2,1990-01-01,1,8,0,2003-07-03,,2007-07-19,2003-07-03,293056.00,091088.00,292978.00,091480.00,10
      // 15,"I",212,14200711,"RIVERMEAD ROAD","","EXETER","DEVON","ENG"
      // 21,"I",3452,10023117082,1,2,2008-03-26,,292895.12,091367.63,1,1110,2008-03-26,,2012-04-03,2008-03-26,"S","EX2 4RH",0
      // 23,"I",3453,10023117082,"1110X014144940","30895182",,"7666VC",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3454,10023117082,"1110X600059474","osgb4000000025321066",4,"7666MI",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3455,10023117082,"1110X700059474","osgb1000012316569",3,"7666MT",2008-03-26,,2012-04-03,2008-03-26
      // 23,"I",3456,10023117082,"1110X900059474","osgb1000002274376928",4,"7666MA",2008-03-26,,2012-04-03,2008-03-26
      // 24,"I",3457,10023117082,"1110L000163495","ENG",1,2008-03-26,,2008-03-26,2008-03-26,,"",,"","",1,"",,"","",14200711,"1","","","Y"
      // 28,"I",3458,10023117082,,8754092,"","","","",1,"","RIVERMEAD ROAD","","","EXETER","EX2 4RH","S","","","","","","",2014-04-22,2008-03-26,,2012-04-03,2008-03-26
      assert(addressesProduced(9) === DbAddress("GB10023117082", List("1 Rivermead Road"), "Exeter", "EX2 4RH", "GB-ENG"))

      // 11,"I",1391,14200938,1,1110,2,1990-01-01,1,8,0,1995-06-20,,2007-07-19,1995-06-20,292721.00,092367.00,293484.00,091739.00,10
      // 15,"I",1392,14200938,"WONFORD ROAD","","EXETER","DEVON","ENG"
      // 21,"I",3460,10023118022,1,2,2011-07-12,100040241198,293270.00,091854.00,1,1110,2009-07-03,,2014-02-26,2009-07-03,"C","EX2 4UD",0
      // 23,"I",3461,10023118022,"1110X014126401","6839763000",,"7666VC",2009-07-03,,2011-07-12,2009-07-03
      // 24,"I",3462,10023118022,"1110L000164456","ENG",1,2009-07-03,,2014-02-26,2009-07-03,,"",,"","ANNEXE",49,"",,"","",14200938,"1","","","N"
      // 32,"I",3463,10023118022,"1110C000059544","R","AddressBase Premium Classification Scheme",1.0,2009-07-03,,2011-07-12,2009-07-03
      assert(addressesProduced(10) === DbAddress("GB10023118022", List("Annexe", "49 Wonford Road"), "Exeter", "EX2 4UD", "GB-ENG"))

      // 11,"I",1951,14200067,1,1110,2,1990-01-01,1,8,0,2003-06-20,,2007-07-19,2003-06-20,292760.00,094294.00,292621.00,094195.00,10
      // 15,"I",1952,14200067,"BEECH AVENUE","","EXETER","DEVON","ENG"
      // 21,"I",3503,10023117050,1,2,2008-03-13,,292687.13,094177.44,1,1110,2008-03-13,,2012-04-03,2008-03-13,"S","EX4 6HE",0
      // 23,"I",3504,10023117050,"1110X014220197","5419240000",,"7666VC",2008-03-13,,2012-04-03,2008-03-13
      // 23,"I",3505,10023117050,"1110X600059491","osgb4000000025306215",5,"7666MI",2014-02-23,,2014-04-04,2014-02-23
      // 23,"I",3506,10023117050,"1110X700059491","osgb1000012326378",4,"7666MT",2014-02-23,,2014-04-04,2014-02-23
      // 23,"I",3507,10023117050,"1110X900059491","osgb1000002274501683",4,"7666MA",2014-02-23,,2014-05-20,2014-02-23
      // 24,"I",3508,10023117050,"1110L000163459","ENG",1,2008-03-13,,2008-03-13,2008-03-13,,"",,"","",,"",,"","BEECH COTTAGE ANNEXE",14200067,"1","","","Y"
      // 28,"I",3509,10023117050,,52124252,"","","","BEECH COTTAGE",,"","BEECH AVENUE","","","EXETER","EX4 6HE","S","","","","","","",2014-04-22,2014-02-23,,2014-04-04,2014-02-23
      // 32,"I",3510,10023117050,"1110C000059491","RD03","AddressBase Premium Classification Scheme",1.0,2008-03-13,,2012-04-03,2008-03-13
      assert(addressesProduced(16) === DbAddress("GB10023117050", List("Beech Cottage", "Beech Avenue"), "Exeter", "EX4 6HE", "GB-ENG"))

      // 11,"I",2747,14200165,1,1110,2,1990-01-01,1,8,0,2003-10-23,,2007-07-19,2003-10-23,291242.00,091755.00,291442.00,091899.00,10
      // 15,"I",2748,14200165,"CHURCH ROAD","ST THOMAS","EXETER","DEVON","ENG"
      // 21,"I",3534,10023118563,1,2,2010-03-04,,291389.00,091904.00,1,1110,2010-03-04,,2013-02-11,2010-03-04,"M","EX2 9DP",10
      // 24,"I",3535,10023118563,"1110L000164998","ENG",1,2010-03-04,,2010-03-04,2010-03-04,,"",,"","",,"",,"","CHAPEL COURT",14200165,"1","","","N"
      // 32,"I",3536,10023118563,"1110C000059540","PP","AddressBase Premium Classification Scheme",1.0,2010-03-04,,2010-03-04,2010-03-04
      assert(addressesProduced(20) === DbAddress("GB10023118563", List("Chapel Court", "Church Road", "St Thomas"), "Exeter", "EX2 9DP", "GB-ENG"))

      // 11,"I",3257,14200708,1,1110,2,1990-01-01,1,8,0,2004-12-08,,2007-07-19,2004-12-08,291536.00,092922.00,291712.00,093103.00,10
      // 15,"I",3258,14200708,"RICHMOND ROAD","","EXETER","DEVON","ENG"
      // 21,"I",3537,10023117085,1,2,2008-03-26,100041142593,291571.58,092936.20,1,1110,2008-03-26,,2012-04-03,2008-03-26,"C","EX4 4JF",0
      // 23,"I",3538,10023117085,"1110X014145136","5900902000",,"7666VC",2008-03-26,,2012-04-03,2008-03-26
      // 24,"I",3539,10023117085,"1110L000163498","ENG",1,2008-03-26,,2008-03-26,2008-03-26,,"",,"","GROUND FLOOR FRONT",28,"",,"","",14200708,"1","","","Y"
      // 32,"I",3540,10023117085,"1110C000059471","RD06","AddressBase Premium Classification Scheme",1.0,2008-03-26,,2012-04-03,2008-03-26
      assert(addressesProduced(21) === DbAddress("GB10023117085", List("Ground Floor Front", "28 Richmond Road"), "Exeter", "EX4 4JF", "GB-ENG"))
    }
  }

}
