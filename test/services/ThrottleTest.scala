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

import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class ThrottleTest extends FunSuite with MockitoSugar {

  test(
    """
       Given that time ticks are stubbed to be advancing very slowly,
       When a throttle is used with a periodic setting,
       Then the sleep time would have been period * numItems.
    """) {
    val time = mock[Time]
    val throttle = new Throttle(10, 0, time)

    stub(time.now()) toReturn 1000000L

    val lock = throttle.start()
    lock.waitUntilEndOfCycle(7)
    lock.release()

    verify(time).sleep(70)
  }

  test(
    """
       Given that time ticks are stubbed to be advancing very slowly,
       When a throttle is used with a backoff setting,
       Then the sleep time would have been period * numItems.
    """) {
    val time = mock[Time]
    val throttle = new Throttle(0, 10, time)

    stub(time.now()) toReturn 1000000L

    val lock = throttle.start()

    stub(time.now()) toReturn 1000050L

    lock.waitUntilEndOfCycle(7)
    lock.release()

    verify(time).sleep(450)
  }
}
