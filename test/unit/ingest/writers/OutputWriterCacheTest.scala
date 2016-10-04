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

package ingest.writers

import java.util.Date

import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import uk.gov.hmrc.address.osgb.DbAddress

@RunWith(classOf[JUnitRunner])
class OutputWriterCacheTest extends FunSuite with MockitoSugar {

  test("existingTargetThatIsNewerThan") {
    val now = new Date()
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    cache.existingTargetThatIsNewerThan(now)
    verify(peer).existingTargetThatIsNewerThan(now)
  }

  test("begin") {
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    cache.begin()
    verify(peer).begin()
  }

  test("end true") {
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    cache.end(true)
    verify(peer).end(true)
  }

  test("end false") {
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    cache.end(false)
    verify(peer).end(false)
  }

  test("output") {
    val a = DbAddress("GB100040230002", List("6 Prospect Gardens"), Some("Exeter"), "EX4 6TA", Some("GB-ENG"),
      Some("UK"), Some(1110), Some("en"), Some(3), Some(1), None, None, None)
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    cache.output(a)
    verify(peer).output(a)
  }

  test("size, get, remove") {
    val a = DbAddress("GB100040230002", List("6 Prospect Gardens"), Some("Exeter"), "EX4 6TA", Some("GB-ENG"),
      Some("UK"), Some(1110), Some("en"), Some(3), Some(1), None, None, None)
    val peer = mock[OutputWriter]
    val cache = new OutputWriterCache(peer)
    assert(cache.get(100040230002L) === None)
    assert(cache.isEmpty)
    assert(cache.size === 0)

    cache.output(a)

    assert(cache.get(100040230002L) === Some(a))
    assert(!cache.isEmpty)
    assert(cache.size === 1)

    cache.remove(100040230002L)
    assert(cache.get(100040230002L) === None)
    assert(cache.isEmpty)
    assert(cache.size === 0)
  }

}
