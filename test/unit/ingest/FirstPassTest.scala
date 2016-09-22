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

import addressbase.OSCsv
import ingest.algorithm.Algorithm
import ingest.writers.OutputWriter
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import services.exec.{Continuer, WorkQueue}
import services.model.{StateModel, StatusLogger}
import uk.co.hmrc.address.osgb.DbAddress
import uk.co.hmrc.address.services.CsvParser
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class FirstPassTest extends FunSuite with MockitoSugar {

  // sample data here is in the new format
  OSCsv.setCsvFormat(2)

  // test data is long so disable scalastyle check
  // scalastyle:off

  class context(data: String) {
    val csv = CsvParser.split(data)
    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val worker = new WorkQueue(status)
    val dummyOut = mock[OutputWriter]
    val continuer = mock[Continuer]
    val lock = new SynchronousQueue[Boolean]()
    val model = new StateModel()
    val forwardData = ForwardData.chronicleInMemoryForUnitTest("DPA")
  }

  test(
    """Given an OS-StreetDescriptor
       the street table will be augmented correctly
    """) {
    new context(
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""
    ) {
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.streets.size === 1)
      assert(firstPass.forwardData.streets.get(48504236L) === "A|A76 T From Co-Operative Offices to Castle Place||New Cumnock")
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPAs, 1 streets.")
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
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.streets.size === 1)
      assert(firstPass.forwardData.streets.get(47208194L) === "2|Cwmduad to Cynwyl Elfed|Cwmduad|Carmarthen")
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPAs, 1 streets.")
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
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.streets.size === 1)
      assert(firstPass.forwardData.streets.get(48504236L) === "2|A76 T From Co-Operative Offices to Castle Place||New Cumnock")
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPAs, 1 streets.")
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
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.streets.size === 1)
      assert(firstPass.forwardData.streets.get(48504236L) === "2|A76 T From Co-Operative Offices to Castle Place||New Cumnock")
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 0 DPAs, 1 streets.")
    }
  }


  test(
    """Given a v1 OS-BLPU
       the BLPU table will be augmented correctly
    """) {
    new context(
      """
        |10,"GeoPlace",9999,2011-07-08,1,2011-07-08,16:00:30,"1.0","F"
        |21,"I",521480,320077134,1,2,2011-09-09,,354661.00,702526.00,1,9066,1992-06-10,,2004-08-10,2004-08-09,"S","KY10 2PY",0
        | """.stripMargin
    ) {
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.blpu.size === 1)
      assert(firstPass.forwardData.blpu.get(320077134L) === "|KY10 2PY|1|J|9066")
      assert(firstPass.forwardData.postcodeLCCs.size === 1)
      assert(firstPass.forwardData.postcodeLCCs.get("KY10 2PY") === "9066")
      assert(firstPass.sizeInfo === "First pass obtained 1 BLPUs, 0 DPAs, 0 streets.")
    }
  }


  test(
    """Given a v2 OS-BLPU
       the BLPU table will be augmented correctly
    """) {
    new context(
      """
        |10,"NAG Hub - GeoPlace",9999,2016-02-19,0,2016-02-19,23:47:05,"2.0","F"
        |21,"I",246843,100091275899,1,2,2012-08-06,100091660014,624285.00,221683.00,51.8486717,1.2550278,2,1560,"E",2007-12-21,,2016-02-10,2002-08-21,"D","CO14 8RX",0
        |21,"I",277974,35008288,8,4,2016-05-23,,288316.00,696943.00,56.1520438,-3.7994337,2,9056,"S",2012-04-27,2016-05-31,2016-06-17,2000-07-01,"L","FK12 5AG",0
        |21,"I",277956,35008270,1,,,,288369.00,696947.00,56.1520921,-3.7985827,2,9056,"S",2012-04-27,,2016-02-10,1985-04-01,"D","FK12 5AG",0
        | """.stripMargin
    ) {
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.blpu.size === 3)
      assert(firstPass.forwardData.blpu.get(100091275899L) === "100091660014|CO14 8RX|1|E|1560")
      assert(firstPass.forwardData.blpu.get(35008288L) === "|FK12 5AG|8|S|9056")
      assert(firstPass.forwardData.blpu.get(35008270L) === "|FK12 5AG|1|S|9056")
      assert(firstPass.sizeInfo === "First pass obtained 3 BLPUs, 0 DPAs, 0 streets.")
    }
  }

  test(
    """Given a v2 OS-BLPU with
       * a singular postcode
       * a 7655-only postcode
       * a singular postcode plus 7655
       * a plural postcode
       the PostcodeLCC table will be augmented correctly for each case
    """) {
    new context(
      """
        |10,"NAG Hub - GeoPlace",9999,2016-02-19,0,2016-02-19,23:47:05,"2.0","F"
        |21,"I",277974,35008288,8,4,2016-05-23,,288316.00,696943.00,56.1520438,-3.7994337,2,9056,"S",2012-04-27,2016-05-31,2016-06-17,2000-07-01,"L","FK12 5AG",0
        |21,"I",277956,35008270,1,,,,288369.00,696947.00,56.1520921,-3.7985827,2,9056,"S",2012-04-27,,2016-02-10,1985-04-01,"D","FK12 5AG",0
        |
        |21,"I",219526,10091854748,1,,,217112644,524256.00,180421.00,51.5090666,-.2108968,2,7655,"E",2014-09-12,,2016-02-10,2012-03-19,"D","W11 4NS",0
        |21,"I",219527,10091854749,1,,,217112644,524256.00,180421.00,51.5090666,-.2108968,2,7655,"E",2014-09-12,,2016-02-10,2012-03-19,"D","W11 4NS",0
        |
        |21,"I",208548,10091831194,1,,,100081051354,482936.00,174686.00,51.4650800,-.8074774,1,7655,"E",2014-09-12,,2016-02-10,2012-03-19,"D","RG10 0NY",0
        |21,"I",587523,100081051353,1,2,2001-04-24,,483056.00,174697.00,51.4651614,-.8057478,1,355,"E",2007-12-18,,2016-02-10,2001-04-24,"D","RG10 0NY",0
        |
        |21,"I",300224,68148277,1,2,2001-02-17,,529955.00,153714.00,51.2677712,-.1385868,1,3625,"E",2007-12-27,,2016-02-10,2001-02-17,"D","RH1 3DB",0
        |21,"I",970693,100061588712,1,2,2007-06-15,,530946.25,153571.07,51.2662599,-.1244385,2,3645,"E",2007-06-22,,2016-02-10,2001-06-21,"D","RH1 3DB",2
        | """.stripMargin
    ) {
      when(continuer.isBusy) thenReturn true

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.postcodeLCCs.size === 3)
      assert(firstPass.forwardData.postcodeLCCs.get("FK12 5AG") === "9056")
      assert(firstPass.forwardData.postcodeLCCs.get("RG10 0NY") === "355")
      assert(firstPass.forwardData.postcodeLCCs.get("RH1 3DB") === Ingester.PostcodeLCC.Plural)
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
      when(continuer.isBusy) thenReturn true

      val out = new OutputWriter {
        def existingTargetThatIsNewerThan(date: Date) = None

        def begin() {}

        def output(out: DbAddress) {
          assert(out.id === "GB9051119283")
          assert(out.lines === List("1 Upper Kennerty Mill Cottages"))
          assert(out.town === "Peterculter")
          assert(out.postcode === "AB14 0LQ")
        }

        def end(completed: Boolean) = model
      }

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          firstPass.processFile(csv, out)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.uprns.size === 1)
      assert(firstPass.forwardData.uprns.contains(9051119283L))
      assert(firstPass.sizeInfo === "First pass obtained 0 BLPUs, 1 DPAs, 0 streets.")
    }
  }

  test(
    """Given that the task is in a stopping state
       then no records will be processed
    """) {
    new context(
      """15,"I",31068,48504236,"A76 T FROM CO-OPERATIVE OFFICES TO CASTLE PLACE","","NEW CUMNOCK","EAST AYRSHIRE","ENG""""
    ) {
      when(continuer.isBusy) thenReturn false

      val firstPass = new FirstPass(dummyOut, continuer, Algorithm(), forwardData)
      worker.push("testing", {
        continuer =>
          lock.put(true)
          worker.abort()
          firstPass.processFile(csv, dummyOut)
      })

      lock.take()
      worker.awaitCompletion()
      assert(firstPass.forwardData.streets.size === 0)
    }
  }

}
