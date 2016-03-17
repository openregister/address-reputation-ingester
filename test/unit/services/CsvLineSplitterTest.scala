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

package services

import org.scalatest.FunSuite

class CsvLineSplitterTest extends FunSuite {
  val expectedLine1 = List("10", "NAG Hub - GeoPlace", "9999", "2014-06-07", "1", "2014-06-07", "09:01:38", "1.0", "F")
  val expectedLine2 = List("11", "I", "1", "14200759", "1", "1110", "2", "1990-01-01", "1", "8", "0",
    "2003-07-02", "", "2007-07-19", "2003-07-02", "292906.00", "093301.00", "292968.00", "093238.00", "10")
  val expectedLine3 = List("15", "I", "2", "14200759", "SILVER LANE", "", "EXETER", "DEVON", "ENG")

  def getResource(relativePath: String) = {
    val is = getClass.getClassLoader.getResourceAsStream(relativePath)
    assert(is != null)
    is
  }

  test("csv line splitter should correctly parse the first few lines of a sample file") {
    val csv = new CsvLineSplitter(getResource("SX9090-first20.csv"))
    assert(csv.hasNext)
    assert(csv.next().toList === expectedLine1)
    assert(csv.hasNext)
    assert(csv.next().toList === expectedLine2)
    assert(csv.hasNext)
    assert(csv.next().toList === expectedLine3)
    assert(csv.hasNext)
    for (i <- 4 to 20) {
      assert(csv.hasNext, i)
      csv.next()
    }
    assert(!csv.hasNext)
  }

  test("pimped-Scala csv line splitter should correctly parse the first few lines of a sample file") {
    val csv = CsvParser.splitResource("SX9090-first20.csv")
    val three = csv.take(3).toList.map(_.toList)
    assert(three === List(expectedLine1, expectedLine2, expectedLine3))
    assert(csv.hasNext)
    for (i <- 4 to 20) {
      assert(csv.hasNext, i)
      csv.next()
    }
    assert(!csv.hasNext)
  }

  test("pimped-Scala csv line splitter should throw an exception for missing resources") {
    val e = intercept[IllegalArgumentException] {
      val csv = CsvParser.splitResource("no-such-file.txt")
    }
    assert(e.getMessage.contains("no-such-file.txt"))
  }
}
