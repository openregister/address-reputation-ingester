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

package services.ingester.converter

import java.io.File

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import uk.co.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class LoadZipTest extends FunSuite with Matchers with MockitoSugar {

  test(
    """given a zip archive containing one file,
       zipReader should return an iterator over the CSV lines
    """) {
    val sample = new File(getClass.getClassLoader.getResource("SX9090-first20.zip").getFile)
    val logger = new StubLogger
    val zip = LoadZip.zipReader(sample, logger)
    assert(zip.hasNext)
    val it = zip.next
    assert(it.zipEntry.getName === "SX9090-first20.csv")
    for (i <- 0 until 19) {
      it.next
    }
    assert(it.hasNext)
    assert(it.next.mkString(",") === "15,I,134,14200774,SOUTHPORT AVENUE,,EXETER,DEVON,ENG")
    assert(!it.hasNext)

    assert(!zip.hasNext)
    zip.close()
  }

  test(
    """given a zip archive containing three files,
       zipReader should return three iterators over the CSV lines
    """) {
    val sample = new File(getClass.getClassLoader.getResource("3files.zip").getFile)
    val logger = new StubLogger
    val zip = LoadZip.zipReader(sample, logger)
    assert(zip.hasNext)
    val it1 = zip.next
    assert(it1.zipEntry.getName === "SX9090-first20.csv")
    for (i <- 0 until 19) {
      it1.next
    }
    assert(it1.hasNext)
    assert(it1.next.mkString(",") === "15,I,134,14200774,SOUTHPORT AVENUE,,EXETER,DEVON,ENG")
    assert(!it1.hasNext)

    assert(zip.hasNext)
    val it2 = zip.next
    assert(it2.zipEntry.getName === "invalid15.csv")
    assert(it2.hasNext)
    assert(it2.next.mkString(",") === "15,I,31068,48504236")
    assert(!it2.hasNext)

    assert(zip.hasNext)
    val it3 = zip.next
    assert(it3.zipEntry.getName === "invalid24.csv")
    assert(it3.hasNext)
    assert(it3.next.mkString(",") === "24,I,913236")
    assert(!it3.hasNext)

    assert(!zip.hasNext)
    zip.close()
  }
}
