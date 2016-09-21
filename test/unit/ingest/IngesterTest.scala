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

import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.exec.WorkQueue
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
          new Ingester(continuer, Algorithm(), model, status, fd).ingest(mockFile, dummyOut)
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
          new Ingester(continuer, Algorithm(), model, status, ForwardData.chronicleInMemoryForUnitTest("DPA")).ingest(mockFile, dummyOut)
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
          result = new Ingester(continuer, Algorithm(), model, status, ForwardData.chronicleInMemoryForUnitTest("DPA")).ingest(mockFile, dummyOut)
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
      val sample = new File(getClass.getClassLoader.getResource("exeter/1/sample/addressbase-premium-csv-sample-data.zip").getFile)
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
          new Ingester(continuer, Algorithm(), model, status, ForwardData.simpleHeapInstance("DPA")).ingest(List(sample), out)
          lock.put(true)
      })

      lock.take()
      worker.awaitCompletion()

      assert(logger.infos.map(_.message) === List(
        "Info:Starting testing.",
        "Info:Starting first pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} took {}.",
        "Info:First pass obtained 48737 BLPUs, 42874 DPAs, 1686 streets.",
        "Info:First pass complete after {}.",
        "Info:Starting second pass through 1 files.",
        "Info:Reading zip entry sx9090.csv...",
        "Info:Reading from 1 CSV files in {} took {}.",
        "Info:Second pass processed 42874 DPAs, 5863 LPIs.", // n.b. there are 53577 LPIs in the sample file
        "Info:Ingester finished after {}.",
        "Info:Finished testing after {}."
      ))
      assert(addressesProduced.size === 48737)
      assert(!closed) // the writer is closed at a higher scope level

      // 21,"I",28224,10013050866,8,,,,292260.00,092751.94,50.7243583,-3.5277673,1,1110,"E",2007-10-24,2014-04-03,2016-02-10,2007-06-27,"L","EX1 1GE",0
      // 24,"I",55850,10013050866,"1110L000160762","ENG",8,2007-10-24,2014-04-03,2016-02-10,2007-06-27,,"",,"","",9,"",,"","",14201579,"1","","","Y"
      // 32,"I",173442,10013050866,"1110C000040672","CR08","AddressBase Premium Classification Scheme",1.0,2007-10-24,2014-04-03,2016-02-10,2007-06-27
      // 23,"I",252628,10013050866,"1110X709841935","osgb1000002070809050",3,"7666MT",2014-04-03,2014-04-03,2016-02-07,2008-03-02
      // 23,"I",252629,10013050866,"1110X609601862","osgb4000000025295481",8,"7666MI",2014-04-03,2014-04-03,2016-02-07,2009-10-14
      // 23,"I",252630,10013050866,"1110X111845090","E05003502",,"7666OW",2014-04-03,2014-04-03,2016-02-07,2016-02-07
      assert(addressesProduced(0) === DbAddress("GB10013050866", List("9 Princesshay"), Some("Exeter"), "EX1 1GE", Some("GB-ENG"), Some(1110)))

      // 21,"I",30646,10091471879,1,2,2015-10-28,10013039199,292468.71,093210.65,50.7285207,-3.5249454,1,1110,"E",2015-10-28,,2016-02-10,2015-10-28,"C","EX4 6AY",0
      // 24,"I",55869,10091471879,"1110L000174404","ENG",1,2015-10-28,,2016-02-07,2015-10-28,,"",,"","FLAT 1",2,"",,"","",14200685,"1","","",""
      // 32,"I",161142,10091471879,"1110C000067748","RD06","AddressBase Premium Classification Scheme",1.0,2015-10-28,,2016-02-06,2015-10-28
      // 23,"I",284656,10091471879,"1110X609031489","osgb4000000025306108",3,"7666MI",2016-02-07,,2016-02-07,2005-09-16
      // 23,"I",284657,10091471879,"1110X709260134","osgb1000012323862",5,"7666MT",2016-02-07,,2016-02-07,2015-11-22
      // 23,"I",284658,10091471879,"1110X113362044","E05003503",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      assert(addressesProduced(3) === DbAddress("GB10091471879", List("Flat 1","2 Queens Crescent"), Some("Exeter"), "EX4 6AY", Some("GB-ENG"), Some(1110))) //TODO This is not really correct

      // 21,"I",30665,10091472481,1,2,2016-01-06,10091472482,291052.00,092182.00,50.7190093,-3.5447065,1,1110,"E",2016-01-07,,2016-02-06,2016-01-06,"L","EX4 1BU",0
      // 24,"I",55896,10091472481,"1110L000175006","ENG",1,2016-01-07,,2016-02-07,2016-01-06,,"",,"","UNIT 1",25,"",,"","",14200521,"1","","","Y"
      // 32,"I",202963,10091472481,"1110C000067922","CI","AddressBase Premium Classification Scheme",1.0,2016-01-07,,2016-02-06,2016-01-06
      // 23,"I",284721,10091472481,"1110X113362085","E05003506",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      // 23,"I",284722,10091472481,"1110X709141134","osgb1000012293093",6,"7666MT",2016-02-07,,2016-02-07,2012-04-04
      // 23,"I",284723,10091472481,"1110X607370707","osgb4000000025320927",3,"7666MI",2016-02-07,,2016-02-07,2005-09-16
      assert(addressesProduced(9) === DbAddress("GB10091472481", List("Unit 1", "25 Manor Road"), Some("Exeter"), "EX4 1BU", Some("GB-ENG"), Some(1110)))

      // 21,"I",30651,10091471884,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55909,10091471884,"1110L000174409","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","ROOM 4",6,"",,"","BARING HOUSE",14200047,"1","","",""
      // 32,"I",161160,10091471884,"1110C000067762","CO01","AddressBase Premium Classification Scheme",1.0,2015-11-04,,2016-02-10,2015-11-02
      // 32,"I",210439,10091471884,"1110C800644124","203","VOA Special Category",1.0,2015-12-01,,2016-02-06,2015-08-10
      // 32,"I",211723,10091471884,"1110C801859368","CO","VOA Primary Description",1.0,2015-12-01,,2016-02-06,2015-08-10
      // 23,"I",284673,10091471884,"1110X042576970","9901789000",,"7666VN",2015-12-01,,2016-02-10,2015-08-10
      // 23,"I",284674,10091471884,"1110X113362049","E05003497",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      // 23,"I",284675,10091471884,"1110X709212837","osgb1000012341771",6,"7666MT",2016-02-07,,2016-02-07,2015-11-24
      // 23,"I",284676,10091471884,"1110X609829441","osgb4000000025306059",4,"7666MI",2016-02-07,,2016-02-07,2005-09-16
      assert(addressesProduced(10) === DbAddress("GB10091471884", List("Room 4 Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some(1110)))

      // 21,"I",30653,10091471886,1,2,2015-11-02,100041044079,293224.00,092506.00,50.7223255,-3.5140436,1,1110,"E",2015-11-04,,2016-02-10,2015-11-02,"L","EX1 1TL",0
      // 24,"I",55942,10091471886,"1110L000174411","ENG",1,2015-11-04,,2016-02-10,2015-11-02,,"",,"","OFFICE 7",6,"",,"","BARING HOUSE",14200047,"1","","",""
      // 32,"I",161150,10091471886,"1110C000067764","CO01","AddressBase Premium Classification Scheme",1.0,2015-11-04,,2016-02-10,2015-11-02
      // 32,"I",192285,10091471886,"1110C802631340","CO","VOA Primary Description",1.0,2015-12-01,,2016-02-06,2015-08-10
      // 32,"I",210426,10091471886,"1110C800643874","203","VOA Special Category",1.0,2015-12-01,,2016-02-06,2015-08-10
      // 23,"I",284681,10091471886,"1110X042574954","9247586000",,"7666VN",2015-12-01,,2016-02-10,2015-08-10
      // 23,"I",284682,10091471886,"1110X113362051","E05003497",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      // 23,"I",284683,10091471886,"1110X709148608","osgb1000012341771",6,"7666MT",2016-02-07,,2016-02-07,2015-11-24
      // 23,"I",284684,10091471886,"1110X607047972","osgb4000000025306059",4,"7666MI",2016-02-07,,2016-02-07,2005-09-16
      assert(addressesProduced(16) === DbAddress("GB10091471886", List("Office 7 Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some(1110)))

      // 21,"I",51397,10023117655,1,2,2008-11-24,100041124570,293907.43,092597.69,50.7232751,-3.5043915,1,1110,"E",2008-11-28,,2016-02-10,2008-11-24,"C","EX1 2SN",0
      // 24,"I",56013,10023117655,"1110L000164082","ENG",1,2008-11-28,,2016-02-10,2008-11-24,,"",,"","SCHOOL KITCHEN",,"",,"","ST MICHAELS CE PRIMARY SCHOOL",14200767,"1","","","N"
      // 32,"I",203835,10023117655,"1110C000000097","CE03","AddressBase Premium Classification Scheme",1.0,2008-11-28,,2016-02-10,2008-11-24
      // 23,"I",259372,10023117655,"1110X708316566","osgb1000002070792087",5,"7666MT",2016-02-07,,2016-02-07,2009-07-01
      // 23,"I",259373,10023117655,"1110X111454302","E05003495",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      // 23,"I",259374,10023117655,"1110X608734357","osgb4000000025306097",5,"7666MI",2016-02-07,,2016-02-07,2008-11-09
      assert(addressesProduced(20) === DbAddress("GB10023117655", List("School Kitchen St Michaels Ce Primary School", "South Lawn Terrace"), Some("Exeter"), "EX1 2SN", Some("GB-ENG"), Some(1110)))

      // 21,"I",52259,10023119039,1,2,2010-09-15,100040234793,292804.00,092311.00,50.7204951,-3.5199347,1,1110,"E",2010-09-16,,2016-02-10,2010-09-15,"C","EX2 4LA",0
      // 24,"I",56016,10023119039,"1110L000165474","ENG",1,2010-09-16,,2016-02-10,2010-09-15,,"",,"","ANNEXE",12,"",,"","",14200790,"1","","",""
      // 32,"I",200223,10023119039,"1110C000000030","R","AddressBase Premium Classification Scheme",1.0,2010-09-16,,2016-02-10,2010-09-15
      // 23,"I",263324,10023119039,"1110X111457564","E05003504",,"7666OW",2016-02-07,,2016-02-07,2016-02-07
      // 23,"I",263325,10023119039,"1110X608411579","osgb4000000025306065",3,"7666MI",2016-02-07,,2016-02-07,2005-09-16
      // 23,"I",263326,10023119039,"1110X708350160","osgb5000005167742577",1,"7666MT",2016-02-07,,2016-02-07,2015-11-24
      assert(addressesProduced(21) === DbAddress("GB10023119039", List("Annexe" , "12 St Leonards Road"), Some("Exeter"), "EX2 4LA", Some("GB-ENG"), Some(1110)))
    }
  }

}
