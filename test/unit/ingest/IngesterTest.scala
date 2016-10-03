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

import ingest.Ingester.{Blpu, PostcodeLCC}
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
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
    assert(found.last.getPath.endsWith("/exeter/1/sample/addressbase-premium-csv-sample-data.zip"))
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
          new Ingester(continuer, Algorithm(), model, status, fd).ingestFrom(mockFile, dummyOut)
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
          new Ingester(continuer, Algorithm(), model, status, ForwardData.chronicleInMemoryForUnitTest("DPA")).ingestFrom(mockFile, dummyOut)
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
          result = new Ingester(continuer, Algorithm(), model, status, ForwardData.chronicleInMemoryForUnitTest("DPA")).ingestFrom(mockFile, dummyOut)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()
      assert(result === true)
    }
  }


  test(
    """given a zip archive containing one file,
       Ingester should iterate over the CSV lines it contains, using 'prefer-DPA'
    """) {
    new context {
      val sample = new File(getClass.getClassLoader.getResource("exeter/1/sample/addressbase-premium-csv-sample-data.zip").getFile)
      val addressesProduced = new mutable.HashMap[String, DbAddress]()
      var closed = false

      val out = new OutputWriter {
        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(a: DbAddress) {
          addressesProduced(a.id) = a
        }

        def end(completed: Boolean) = {
          closed = true
          model
        }
      }

      worker.push("testing", {
        continuer =>
          new Ingester(continuer, Algorithm(prefer = "DPA"), model, status, ForwardData.simpleHeapInstance("DPA")).ingestFiles(List(sample), out)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()

      assert(logger.infos.map(_.message) === List(
        "Info:Starting testing.",
        "Info:Starting first pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} (1 of 1) took {}.",
        "Info:First pass obtained 48737 BLPUs, 42874 DPAs, 1686 streets, 1686/0 street descriptors.",
        "Info:First pass complete after {}.",
        "Info:Default LCC reduction altered 282 BLPUs and took {}.",
        "Info:Starting second pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} (1 of 1) took {}.",
        "Info:Second pass processed 42874 DPAs, 5863 LPIs.", // n.b. there are 53577 LPIs in the sample file
        "Info:Ingester finished after {}.",
        "Info:Finished testing after {}."
      ))
      assert(addressesProduced.size === 48737)
      assert(!closed) // the writer is closed at a higher scope level

      // 21,"I",11018,100040230002,1,3,2011-07-12,,293103.00,093405.00,50.730385,-3.5160181,1,1110,"E",2007-10-24,,2016-02-10,2001-04-04,"D","EX4 6TA",0
      // 24,"I",55848,100040230002,"1110L000137506","ENG",1,2007-10-24,,2016-02-10,2005-06-26,,"",,"","",6,"",,"","",14200678,"1","","","N"
      // 28,"I",139197,100040230002,8788293,"","","","",6,"","PROSPECT GARDENS","","","EXETER","EX4 6TA","S","1G","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB100040230002") === DbAddress("GB100040230002", List("6 Prospect Gardens"), Some("Exeter"), "EX4 6TA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(3), Some(1), None))

      // 21,"I",51722,10023118140,1,2,2009-11-04,10023118154,292063.62,091898.66,50.7166511,-3.5302985,1,1110,"E",2009-11-10,,2016-02-10,2009-11-04,"D","EX2 8DP",0
      // 24,"I",55849,10023118140,"1110L000164577","ENG",1,2009-11-10,,2016-02-10,2009-11-04,3,"",,"","",,"",,"","TERRACINA COURT",14200371,"1","","","Y"
      // 28,"I",127142,10023118140,51606770,"","","","3 TERRACINA COURT",,"","HAVEN ROAD","","","EXETER","EX2 8DP","S","1W","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10023118140") === DbAddress("GB10023118140", List("3 Terracina Court", "Haven Road"), Some("Exeter"), "EX2 8DP", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None))

      // 21,"I",28224,10013050866,8,,,,292260.00,092751.94,50.7243583,-3.5277673,1,1110,"E",2007-10-24,2014-04-03,2016-02-10,2007-06-27,"L","EX1 1GE",0
      // 24,"I",55850,10013050866,"1110L000160762","ENG",8,2007-10-24,2014-04-03,2016-02-10,2007-06-27,,"",,"","",9,"",,"","",14201579,"1","","","Y"
      assert(addressesProduced("GB10013050866") === DbAddress("GB10013050866", List("9 Princesshay"), Some("Exeter"), "EX1 1GE", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(8), Some(8)))

      // 21,"I",30646,10091471879,1,2,2015-10-28,10013039199,292468.71,093210.65,50.7285207,-3.5249454,1,1110,"E",2015-10-28,,2016-02-10,2015-10-28,"C","EX4 6AY",0
      // 24,"I",55869,10091471879,"1110L000174404","ENG",1,2015-10-28,,2016-02-07,2015-10-28,,"",,"","FLAT 1",2,"",,"","",14200685,"1","","",""
      assert(addressesProduced("GB10091471879") === DbAddress("GB10091471879", List("Flat 1", "2 Queens Crescent"), Some("Exeter"), "EX4 6AY", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8))) //TODO This is not really correct

      // 21,"I",30665,10091472481,1,2,2016-01-06,10091472482,291052.00,092182.00,50.7190093,-3.5447065,1,1110,"E",2016-01-07,,2016-02-06,2016-01-06,"L","EX4 1BU",0
      // 24,"I",55896,10091472481,"1110L000175006","ENG",1,2016-01-07,,2016-02-07,2016-01-06,,"",,"","UNIT 1",25,"",,"","",14200521,"1","","","Y"
      assert(addressesProduced("GB10091472481") === DbAddress("GB10091472481", List("Unit 1", "25 Manor Road"), Some("Exeter"), "EX4 1BU", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",30651,10091471884,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55909,10091471884,"1110L000174409","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","ROOM 4",6,"",,"","BARING HOUSE",14200047,"1","","",""
      assert(addressesProduced("GB10091471884") === DbAddress("GB10091471884", List("Room 4, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",30653,10091471886,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55942,10091471886,"1110L000174411","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","OFFICE 7",6,"",,"","BARING HOUSE",14200047,"1","","",""
      assert(addressesProduced("GB10091471886") === DbAddress("GB10091471886", List("Office 7, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",51397,10023117655,1,2,2008-11-24,100041124570,293907.43,092597.69,50.7232751,-3.5043915,1,1110,"E",2008-11-28,,2016-02-10,2008-11-24,"C","EX1 2SN",0
      // 24,"I",56013,10023117655,"1110L000164082","ENG",1,2008-11-28,,2016-02-10,2008-11-24,,"",,"","SCHOOL KITCHEN",,"",,"","ST MICHAELS CE PRIMARY SCHOOL",14200767,"1","","","N"
      assert(addressesProduced("GB10023117655") === DbAddress("GB10023117655", List("School Kitchen, St Michaels Ce Primary School", "South Lawn Terrace"), Some("Exeter"), "EX1 2SN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",399,14200790,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2006-04-07,292588.00,091868.00,50.7164727,-3.5228644,292830.00,092379.00,50.7211112,-3.5195863,10
      // 15,"I",3285,14200790,"ST LEONARDS ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2006-04-07
      // 21,"I",52259,10023119039,1,2,2010-09-15,100040234793,292804.00,092311.00,50.7204951,-3.5199347,1,1110,"E",2010-09-16,,2016-02-10,2010-09-15,"C","EX2 4LA",0
      // 24,"I",56016,10023119039,"1110L000165474","ENG",1,2010-09-16,,2016-02-10,2010-09-15,,"",,"","ANNEXE",12,"",,"","",14200790,"1","","",""
      assert(addressesProduced("GB10023119039") === DbAddress("GB10023119039", List("Annexe", "12 St Leonards Road"), Some("Exeter"), "EX2 4LA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",83,14200227,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-10-13,292439.00,094538.00,50.7404477,-3.5257539,292525.00,094495.00,50.7400771,-3.524523,10
      // 15,"I",3107,14200227,"CURLEW WAY","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-10-13
      // 21,"I",19848,10013036716,8,,,100040210161,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,2014-04-16,2016-02-10,2005-07-07,"C","EX4 4SW",0
      // 21,"I",26673,10013048054,1,2,2011-07-12,100040210161,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,,2016-02-10,2006-04-05,"C","EX4 4SW",0
      // 21,"I",38672,100040210161,1,2,2014-04-14,,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,,2016-02-10,2001-04-04,"D","EX4 4SW",1
      // 24,"I",69329,100040210161,"1110L000132209","ENG",1,2007-10-24,,2016-02-10,2005-06-26,,"",,"","",1,"",,"","",14200227,"1","","","Y"
      // 28,"I",109425,100040210161,8783681,"","","","",1,"","CURLEW WAY","","","EXETER","EX4 4SW","S","1A","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB100040210161") === DbAddress("GB100040210161", List("1 Curlew Way"), Some("Exeter"), "EX4 4SW", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None))

      // 11,"I",1297,14203041,1,1110,2,2013-01-29,1,8,0,2013-01-29,,2016-02-06,2013-01-29,293488.00,090746.00,50.7065522,-3.5097963,293446.14,090672.24,50.7058814,-3.5103676,10
      // 15,"I",1913,14203041,"GATE REACH","","EXETER","DEVON","ENG",2013-01-29,,2016-02-06,2013-01-29
      // 21,"I",54271,10023122465,1,2,2013-01-31,,293505.38,090643.08,50.7056301,-3.5095205,1,1110,"E",2014-02-21,,2016-02-10,2014-02-20,"D","EX2 6GA",0
      // 24,"I",105754,10023122465,"1110L000168898","ENG",1,2014-02-21,,2016-02-10,2014-02-20,,"",,"","",15,"",,"","",14203041,"1","","","Y"
      // 28,"I",109426,10023122465,52995200,"","","","",15,"","GATE REACH","","","EXETER","EX2 6GA","S","1T","","","","","","",2016-01-18,2014-04-01,,2016-02-10,2013-12-16
      assert(addressesProduced("GB10023122465") === DbAddress("GB10023122465", List("15 Gate Reach"), Some("Exeter"), "EX2 6GA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None))

      // 11,"I",1134,14200580,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-11-16,291119.00,094104.00,50.7362999,-3.5443259,292320.00,092881.00,50.7255296,-3.5269553,10
      // 15,"I",2831,14200580,"NEW NORTH ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-11-16
      // 21,"I",52302,10023119082,1,2,2010-09-17,10023119042,291232.36,094165.57,50.7368747,-3.5427382,2,1110,"E",2010-09-18,,2016-02-10,2010-09-17,"D","EX4 4FT",0
      // 24,"I",108495,10023119082,"1110L000165517","ENG",1,2010-09-18,,2016-02-10,2010-09-17,,"",,"","FLAT G.01 BLOCK G",,"",,"","BIRKS HALLS",14200580,"1","","",""
      // 28,"I",109427,10023119082,52172489,"","","FLAT G.01 BLOCK G","BIRKS HALL",,"","NEW NORTH ROAD","","","EXETER","EX4 4FT","S","1A","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10023119082") === DbAddress("GB10023119082", List("Flat G.01 Block G, Birks Hall", "New North Road"), Some("Exeter"), "EX4 4FT", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None))

      // 11,"I",1541,14200068,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,1995-06-20,292685.00,093018.00,50.7268288,-3.5218258,292602.00,092926.00,50.7259864,-3.5229745,10
      // 15,"I",2184,14200068,"BELGRAVE ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,1995-06-20
      // 21,"I",30823,10092760043,1,,,10013045079,292653.00,092961.00,50.7263105,-3.5222624,1,7655,"E",2014-09-12,,2016-02-10,2012-12-27,"D","EX1 9UL",0
      // 24,"I",102319,10092760043,"7655L200047949","ENG",1,2014-09-12,,2016-02-10,2012-12-27,,"",,"","PO BOX 795",,"",,"","SUMMERLAND GATE",14200068,"2","","","N"
      // 28,"I",144222,10092760043,52960003,"FLY BE","","","",,"","","","","EXETER","EX1 9UL","L","1A","","","","","","795",2016-01-18,2014-09-12,,2016-02-10,2012-12-27
      //TODO this is rubbish!
      assert(addressesProduced("GB10092760043") === DbAddress("GB10092760043", List(), Some("Exeter"), "EX1 9UL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(1), None))

      // 11,"I",76,14202264,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-03-29,292131.10,090785.06,50.7066528,-3.5290171,291680.33,090701.86,50.705821,-3.5353741,10
      // 15,"I",2384,14202264,"MARSH GREEN ROAD WEST","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-03-29
      // 21,"I",20796,10013038314,1,2,2010-11-09,10023118053,292007.57,090693.68,50.7058084,-3.5307391,1,1110,"E",2007-10-24,,2016-02-10,2005-07-07,"D","EX2 8PN",0
      // 24,"I",59468,10013038314,"1110L000146429","ENG",1,2007-10-24,,2016-02-10,2005-07-07,,"",,"","UNIT 25",39,"",,"","EXETER BUSINESS CENTRE",14202264,"1","","",""
      // 28,"I",117777,10013038314,8764842,"ENVIRONMENTAL INSTRUMENTS","","UNIT 25","EXETER BUSINESS CENTRE",39,"","MARSH GREEN ROAD WEST","","MARSH BARTON TRADING ESTATE","EXETER","EX2 8PN","S","4L","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10013038314") === DbAddress("GB10013038314", List("Unit 25, Exeter Business Centre", "39 Marsh Green Road West", "Marsh Barton Trading Estate"), Some("Exeter"), "EX2 8PN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None))
    }
  }


  test(
    """given a zip archive containing one file,
       Ingester should iterate over the CSV lines it contains, using 'prefer-LPI'
    """) {
    new context {
      val sample = new File(getClass.getClassLoader.getResource("exeter/1/sample/addressbase-premium-csv-sample-data.zip").getFile)
      val addressesProduced = new mutable.HashMap[String, DbAddress]()
      var closed = false

      val out = new OutputWriter {
        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(a: DbAddress) {
          addressesProduced(a.id) = a
        }

        def end(completed: Boolean) = {
          closed = true
          model
        }
      }

      worker.push("testing", {
        continuer =>
          new Ingester(continuer, Algorithm(prefer = "LPI"), model, status, ForwardData.simpleHeapInstance("LPI")).ingestFiles(List(sample), out)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()

      assert(logger.infos.map(_.message) === List(
        "Info:Starting testing.",
        "Info:Starting first pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} (1 of 1) took {}.",
        "Info:First pass obtained 48737 BLPUs, 52475 LPIs, 1686 streets, 1686/0 street descriptors.",
        "Info:First pass complete after {}.",
        "Info:Default LCC reduction altered 282 BLPUs and took {}.",
        "Info:Starting second pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} (1 of 1) took {}.",
        "Info:Second pass processed 0 DPAs, 48737 LPIs.", // n.b. there are 53577 LPIs in the sample file
        "Info:Ingester finished after {}.",
        "Info:Finished testing after {}."
      ))
      assert(addressesProduced.size === 48737)
      assert(!closed) // the writer is closed at a higher scope level

      // 21,"I",11018,100040230002,1,3,2011-07-12,,293103.00,093405.00,50.730385,-3.5160181,1,1110,"E",2007-10-24,,2016-02-10,2001-04-04,"D","EX4 6TA",0
      // 24,"I",55848,100040230002,"1110L000137506","ENG",1,2007-10-24,,2016-02-10,2005-06-26,,"",,"","",6,"",,"","",14200678,"1","","","N"
      // 28,"I",139197,100040230002,8788293,"","","","",6,"","PROSPECT GARDENS","","","EXETER","EX4 6TA","S","1G","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB100040230002") === DbAddress("GB100040230002", List("6 Prospect Gardens"), Some("Exeter"), "EX4 6TA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(3), Some(1), Some(8)))

      // 21,"I",51722,10023118140,1,2,2009-11-04,10023118154,292063.62,091898.66,50.7166511,-3.5302985,1,1110,"E",2009-11-10,,2016-02-10,2009-11-04,"D","EX2 8DP",0
      // 24,"I",55849,10023118140,"1110L000164577","ENG",1,2009-11-10,,2016-02-10,2009-11-04,3,"",,"","",,"",,"","TERRACINA COURT",14200371,"1","","","Y"
      // 28,"I",127142,10023118140,51606770,"","","","3 TERRACINA COURT",,"","HAVEN ROAD","","","EXETER","EX2 8DP","S","1W","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10023118140") === DbAddress("GB10023118140", List("3 Terracina Court", "Haven Road"), Some("Exeter"), "EX2 8DP", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",28224,10013050866,8,,,,292260.00,092751.94,50.7243583,-3.5277673,1,1110,"E",2007-10-24,2014-04-03,2016-02-10,2007-06-27,"L","EX1 1GE",0
      // 24,"I",55850,10013050866,"1110L000160762","ENG",8,2007-10-24,2014-04-03,2016-02-10,2007-06-27,,"",,"","",9,"",,"","",14201579,"1","","","Y"
      assert(addressesProduced("GB10013050866") === DbAddress("GB10013050866", List("9 Princesshay"), Some("Exeter"), "EX1 1GE", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(8), Some(8)))

      // 21,"I",30646,10091471879,1,2,2015-10-28,10013039199,292468.71,093210.65,50.7285207,-3.5249454,1,1110,"E",2015-10-28,,2016-02-10,2015-10-28,"C","EX4 6AY",0
      // 24,"I",55869,10091471879,"1110L000174404","ENG",1,2015-10-28,,2016-02-07,2015-10-28,,"",,"","FLAT 1",2,"",,"","",14200685,"1","","",""
      assert(addressesProduced("GB10091471879") === DbAddress("GB10091471879", List("Flat 1", "2 Queens Crescent"), Some("Exeter"), "EX4 6AY", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",30665,10091472481,1,2,2016-01-06,10091472482,291052.00,092182.00,50.7190093,-3.5447065,1,1110,"E",2016-01-07,,2016-02-06,2016-01-06,"L","EX4 1BU",0
      // 24,"I",55896,10091472481,"1110L000175006","ENG",1,2016-01-07,,2016-02-07,2016-01-06,,"",,"","UNIT 1",25,"",,"","",14200521,"1","","","Y"
      assert(addressesProduced("GB10091472481") === DbAddress("GB10091472481", List("Unit 1", "25 Manor Road"), Some("Exeter"), "EX4 1BU", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",30651,10091471884,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55909,10091471884,"1110L000174409","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","ROOM 4",6,"",,"","BARING HOUSE",14200047,"1","","",""
      assert(addressesProduced("GB10091471884") === DbAddress("GB10091471884", List("Room 4, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",30653,10091471886,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55942,10091471886,"1110L000174411","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","OFFICE 7",6,"",,"","BARING HOUSE",14200047,"1","","",""
      assert(addressesProduced("GB10091471886") === DbAddress("GB10091471886", List("Office 7, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 21,"I",51397,10023117655,1,2,2008-11-24,100041124570,293907.43,092597.69,50.7232751,-3.5043915,1,1110,"E",2008-11-28,,2016-02-10,2008-11-24,"C","EX1 2SN",0
      // 24,"I",56013,10023117655,"1110L000164082","ENG",1,2008-11-28,,2016-02-10,2008-11-24,,"",,"","SCHOOL KITCHEN",,"",,"","ST MICHAELS CE PRIMARY SCHOOL",14200767,"1","","","N"
      assert(addressesProduced("GB10023117655") === DbAddress("GB10023117655", List("School Kitchen, St Michaels Ce Primary School", "South Lawn Terrace"), Some("Exeter"), "EX1 2SN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",399,14200790,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2006-04-07,292588.00,091868.00,50.7164727,-3.5228644,292830.00,092379.00,50.7211112,-3.5195863,10
      // 15,"I",3285,14200790,"ST LEONARDS ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2006-04-07
      // 21,"I",52259,10023119039,1,2,2010-09-15,100040234793,292804.00,092311.00,50.7204951,-3.5199347,1,1110,"E",2010-09-16,,2016-02-10,2010-09-15,"C","EX2 4LA",0
      // 24,"I",56016,10023119039,"1110L000165474","ENG",1,2010-09-16,,2016-02-10,2010-09-15,,"",,"","ANNEXE",12,"",,"","",14200790,"1","","",""
      assert(addressesProduced("GB10023119039") === DbAddress("GB10023119039", List("Annexe", "12 St Leonards Road"), Some("Exeter"), "EX2 4LA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",83,14200227,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-10-13,292439.00,094538.00,50.7404477,-3.5257539,292525.00,094495.00,50.7400771,-3.524523,10
      // 15,"I",3107,14200227,"CURLEW WAY","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-10-13
      // 21,"I",19848,10013036716,8,,,100040210161,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,2014-04-16,2016-02-10,2005-07-07,"C","EX4 4SW",0
      // 21,"I",26673,10013048054,1,2,2011-07-12,100040210161,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,,2016-02-10,2006-04-05,"C","EX4 4SW",0
      // 21,"I",38672,100040210161,1,2,2014-04-14,,292532.00,094529.00,50.740384,-3.5244338,1,1110,"E",2007-10-24,,2016-02-10,2001-04-04,"D","EX4 4SW",1
      // 24,"I",69329,100040210161,"1110L000132209","ENG",1,2007-10-24,,2016-02-10,2005-06-26,,"",,"","",1,"",,"","",14200227,"1","","","Y"
      // 28,"I",109425,100040210161,8783681,"","","","",1,"","CURLEW WAY","","","EXETER","EX4 4SW","S","1A","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB100040210161") === DbAddress("GB100040210161", List("1 Curlew Way"), Some("Exeter"), "EX4 4SW", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",1297,14203041,1,1110,2,2013-01-29,1,8,0,2013-01-29,,2016-02-06,2013-01-29,293488.00,090746.00,50.7065522,-3.5097963,293446.14,090672.24,50.7058814,-3.5103676,10
      // 15,"I",1913,14203041,"GATE REACH","","EXETER","DEVON","ENG",2013-01-29,,2016-02-06,2013-01-29
      // 21,"I",54271,10023122465,1,2,2013-01-31,,293505.38,090643.08,50.7056301,-3.5095205,1,1110,"E",2014-02-21,,2016-02-10,2014-02-20,"D","EX2 6GA",0
      // 24,"I",105754,10023122465,"1110L000168898","ENG",1,2014-02-21,,2016-02-10,2014-02-20,,"",,"","",15,"",,"","",14203041,"1","","","Y"
      // 28,"I",109426,10023122465,52995200,"","","","",15,"","GATE REACH","","","EXETER","EX2 6GA","S","1T","","","","","","",2016-01-18,2014-04-01,,2016-02-10,2013-12-16
      assert(addressesProduced("GB10023122465") === DbAddress("GB10023122465", List("15 Gate Reach"), Some("Exeter"), "EX2 6GA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",1134,14200580,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-11-16,291119.00,094104.00,50.7362999,-3.5443259,292320.00,092881.00,50.7255296,-3.5269553,10
      // 15,"I",2831,14200580,"NEW NORTH ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-11-16
      // 21,"I",52302,10023119082,1,2,2010-09-17,10023119042,291232.36,094165.57,50.7368747,-3.5427382,2,1110,"E",2010-09-18,,2016-02-10,2010-09-17,"D","EX4 4FT",0
      // 24,"I",108495,10023119082,"1110L000165517","ENG",1,2010-09-18,,2016-02-10,2010-09-17,,"",,"","FLAT G.01 BLOCK G",,"",,"","BIRKS HALLS",14200580,"1","","",""
      // 28,"I",109427,10023119082,52172489,"","","FLAT G.01 BLOCK G","BIRKS HALL",,"","NEW NORTH ROAD","","","EXETER","EX4 4FT","S","1A","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10023119082") === DbAddress("GB10023119082", List("Flat G.01 Block G, Birks Halls", "New North Road"), Some("Exeter"), "EX4 4FT", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

      // 11,"I",1541,14200068,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,1995-06-20,292685.00,093018.00,50.7268288,-3.5218258,292602.00,092926.00,50.7259864,-3.5229745,10
      // 15,"I",2184,14200068,"BELGRAVE ROAD","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,1995-06-20
      // 21,"I",30823,10092760043,1,,,10013045079,292653.00,092961.00,50.7263105,-3.5222624,1,7655,"E",2014-09-12,,2016-02-10,2012-12-27,"D","EX1 9UL",0
      // 24,"I",102319,10092760043,"7655L200047949","ENG",1,2014-09-12,,2016-02-10,2012-12-27,,"",,"","PO BOX 795",,"",,"","SUMMERLAND GATE",14200068,"2","","","N"
      // 28,"I",144222,10092760043,52960003,"FLY BE","","","",,"","","","","EXETER","EX1 9UL","L","1A","","","","","","795",2016-01-18,2014-09-12,,2016-02-10,2012-12-27
      assert(addressesProduced("GB10092760043") === DbAddress("GB10092760043", List("Po Box 795, Summerland Gate", "Belgrave Road"), Some("Exeter"), "EX1 9UL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(1), Some(8)))

      // 11,"I",76,14202264,1,1110,2,1990-01-01,1,8,0,2007-10-24,,2016-02-10,2004-03-29,292131.10,090785.06,50.7066528,-3.5290171,291680.33,090701.86,50.705821,-3.5353741,10
      // 15,"I",2384,14202264,"MARSH GREEN ROAD WEST","","EXETER","DEVON","ENG",2007-10-24,,2016-02-06,2004-03-29
      // 21,"I",20796,10013038314,1,2,2010-11-09,10023118053,292007.57,090693.68,50.7058084,-3.5307391,1,1110,"E",2007-10-24,,2016-02-10,2005-07-07,"D","EX2 8PN",0
      // 24,"I",59468,10013038314,"1110L000146429","ENG",1,2007-10-24,,2016-02-10,2005-07-07,,"",,"","UNIT 25",39,"",,"","EXETER BUSINESS CENTRE",14202264,"1","","",""
      // 28,"I",117777,10013038314,8764842,"ENVIRONMENTAL INSTRUMENTS","","UNIT 25","EXETER BUSINESS CENTRE",39,"","MARSH GREEN ROAD WEST","","MARSH BARTON TRADING ESTATE","EXETER","EX2 8PN","S","4L","","","","","","",2016-01-18,2012-04-23,,2016-02-10,2012-03-19
      assert(addressesProduced("GB10013038314") === DbAddress("GB10013038314", List("Unit 25, Exeter Business Centre", "39 Marsh Green Road West"), Some("Exeter"), "EX2 8PN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8)))

    }
  }


  test(
    """
         reduceDefaultedLCCs should
            * replace 7655 with the value obtained from parent UPRN records
            * leave 'normal' LCCs unchanged
         with empty postcode map in the forward data
    """) {
    new context {
      val continuer = mock[Continuer]
      val fd = ForwardData.chronicleInMemoryForUnitTest("DPA")

      val blpu1 = Blpu(None, "FX1 1AA", '1', '2', 'E', 1234)
      val blpu2 = Blpu(None, "FX1 1BB", '1', '2', 'E', 1111)
      val blpu3 = Blpu(Some(2L), "FX1 1CC", '1', '2', 'E', 7655)

      fd.blpu.put(1L, blpu1.pack)
      fd.blpu.put(2L, blpu2.pack)
      fd.blpu.put(3L, blpu3.pack)

      val rfd = new Ingester(continuer, Algorithm(), model, status, mock[ForwardData]).reduceDefaultedLCCs(fd)
      assert(rfd.blpu.get(1L) === blpu1.pack)
      assert(rfd.blpu.get(2L) === blpu2.pack)
      assert(rfd.blpu.get(3L) === Blpu(Some(2L), "FX1 1CC", '1', '2', 'E', blpu2.localCustodianCode).pack)
    }
  }

  test(
    """
         reduceDefaultedLCCs should
            * replace 7655 with the value obtained from adjacent records in the same postcode (with singular LCC)
            * leave 'normal' LCCs unchanged
         with no parent UPRN values
    """) {
    new context {
      val continuer = mock[Continuer]
      val fd = ForwardData.chronicleInMemoryForUnitTest("DPA")

      val blpu1a = Blpu(None, "FX1 1AA", '1', '2', 'E', 1234)
      val blpu1b = Blpu(None, "FX1 1AA", '1', '2', 'E', 7655)
      val blpu2a = Blpu(None, "FX1 1BB", '1', '2', 'E', 1111)
      val blpu2b = Blpu(None, "FX1 1BB", '1', '2', 'E', 2222)
      val blpu2c = Blpu(None, "FX1 1BB", '1', '2', 'E', 7655)

      fd.blpu.put(1L, blpu1a.pack)
      fd.blpu.put(2L, blpu1b.pack)
      fd.blpu.put(3L, blpu2a.pack)
      fd.blpu.put(4L, blpu2b.pack)
      fd.blpu.put(5L, blpu2c.pack)

      fd.postcodeLCCs.put("FX1 1AA", PostcodeLCC(Some(1234)).pack)
      fd.postcodeLCCs.put("FX1 1BB", PostcodeLCC(None).pack)

      val rfd = new Ingester(continuer, Algorithm(), model, status, mock[ForwardData]).reduceDefaultedLCCs(fd)
      assert(rfd.blpu.get(1L) === blpu1a.pack)
      assert(rfd.blpu.get(2L) === blpu1a.pack) // n.b. LCC was changed
      assert(rfd.blpu.get(3L) === blpu2a.pack)
      assert(rfd.blpu.get(4L) === blpu2b.pack)
      assert(rfd.blpu.get(5L) === blpu2c.pack)
    }
  }
}
