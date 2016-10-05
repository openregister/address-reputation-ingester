/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package util

import org.scalatest.FunSuite
import uk.gov.hmrc.address.osgb.DbAddress

class FifoCacheTest extends FunSuite {

  val a01 = DbAddress("GB100040230002", List("6 Prospect Gardens"), Some("Exeter"), "EX4 6TA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(3), Some(1), None, None, None)
  val a02 = DbAddress("GB10023118140", List("3 Terracina Court", "Haven Road"), Some("Exeter"), "EX2 8DP", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, None)
  val a03 = DbAddress("GB10013050866", List("9 Princesshay"), Some("Exeter"), "EX1 1GE", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(8), Some(8), None, None)
  val a04 = DbAddress("GB10091471879", List("Flat 1", "2 Queens Crescent"), Some("Exeter"), "EX4 6AY", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a05 = DbAddress("GB10091472481", List("Unit 1", "25 Manor Road"), Some("Exeter"), "EX4 1BU", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a06 = DbAddress("GB10091471884", List("Room 4, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a07 = DbAddress("GB10091471886", List("Office 7, Baring House", "6 Baring Crescent"), Some("Exeter"), "EX1 1TL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a08 = DbAddress("GB10023117655", List("School Kitchen, St Michaels Ce Primary School", "South Lawn Terrace"), Some("Exeter"), "EX1 2SN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a09 = DbAddress("GB10023119039", List("Annexe", "12 St Leonards Road"), Some("Exeter"), "EX2 4LA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), Some(8), None, None)
  val a10 = DbAddress("GB100040210161", List("1 Curlew Way"), Some("Exeter"), "EX4 4SW", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, None)
  val a11 = DbAddress("GB10023122465", List("15 Gate Reach"), Some("Exeter"), "EX2 6GA", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, None)
  val a12 = DbAddress("GB10023119082", List("Flat G.01 Block G, Birks Hall", "New North Road"), Some("Exeter"), "EX4 4FT", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, None)
  val a13 = DbAddress("GB10092760043", List(), Some("Exeter"), "EX1 9UL", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), None, Some(1), None, None, None)
  val a14 = DbAddress("GB10013038314", List("Unit 25, Exeter Business Centre", "39 Marsh Green Road West", "Marsh Barton Trading Estate"), Some("Exeter"), "EX2 8PN", Some("GB-ENG"), Some("UK"), Some(1110), Some("en"), Some(2), Some(1), None, None, None)

  test("size, get and maximum limit") {
    val cache = new FifoCache[String, DbAddress](5, a => a.id)

    assert(cache.get(a01.id) === None)
    assert(cache.isEmpty)
    assert(cache.size === 0)

    cache.put(a01)

    assert(cache.get(a01.id) === Some(a01))
    assert(cache(a01.id) === a01)
    assert(!cache.isEmpty)
    assert(cache.size === 1)

    cache.put(a02)
    cache.put(a03)
    cache.put(a04)
    cache.put(a05)

    assert(cache.size === 5)

    cache.put(a06)

    assert(cache.size === 5)
    assert(cache.get(a06.id) === Some(a06))
    assert(cache.get(a01.id) === None)
  }

  test("clear") {
    val cache = new FifoCache[String, DbAddress](10, a => a.id)

    cache.put(a01)
    cache.put(a02)

    assert(cache(a01.id) === a01)
    assert(cache(a02.id) === a02)

    cache.clear()

    assert(cache.isEmpty)
  }

}
