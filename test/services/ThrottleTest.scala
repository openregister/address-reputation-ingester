package services

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class ThrottleTest extends FunSuite with MockitoSugar {

  test( """ When a throttle is used with a periodic setting then the sleep time would have been period * numItems.""") {
    val time = mock[Time]
    val throttle = new Throttle(10, 0, time)

    stub(time.now()) toReturn 1000000L

    val lock = throttle.start()
    lock.waitUntilEndOfCycle(7)
    lock.release()

    verify(time).sleep(70)
  }

  test( """ When a throttle is used with a backoff setting then the sleep time would have been period * numItems.""") {
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
