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

import java.time.Clock
import java.util.concurrent.locks.ReentrantLock

// Throttle has two effects:
//
// * all requests that arrive concurrently (per VM) are forced to execute sequentially
//   (this is a consequence of waiting for an in-memory lock)
//
// * each request that uses the throttle only releases the shared lock after
//   a certain period of time has elapsed.
//
// The period of time taken to perform the request's work is computed from the
// sum of two factors
//
// 1. a fixed period cycle, including the time taken to do useful work.
//
// 2. an elastic back-off based on the actual time taken multiplied by some factor.
//
// Either of these can be disabled by setting to zero by their configuration parameter.

class Throttle(initialDelayPerItem: Long, initialBackoffFactor: Float, time: Time) {
  var delayPerItem = initialDelayPerItem
  var backoffFactor = initialBackoffFactor

  /** Grab the lock here, then start some work. */
  def start() = new TimedLock(delayPerItem, backoffFactor, time)
}


class TimedLock(val delayPerItem: Long, backoffFactor: Float, time: Time) {

  FairLock.lock()

  val startTime = time.now()

  def timeTaken = time.now() - startTime

  /**
    * Release the lock after processing 'numItems' records. The waiting time is calculated
    * by applying the number of items to the configuration settings.
    */
  def waitUntilEndOfCycle(numItems: Int) {
    try {
      val timeAlreadyTaken = timeTaken
      val requiredCycleTime = numItems * delayPerItem
      val fixedPeriodRemaining = requiredCycleTime - timeAlreadyTaken
      val elasticPause = (timeAlreadyTaken * backoffFactor).toLong
      val total = fixedPeriodRemaining + elasticPause
      if (total > 0) {
        time.sleep(total)
      }

    } finally {
      FairLock.unlock()
    }
  }

  // For exceptional goodness. Suggestion: call this from a 'finally' block.
  def release() {
    if (FairLock.isHeldByCurrentThread) {
      FairLock.unlock()
    }
  }
}


private object FairLock extends ReentrantLock(true)


/** Provides a seam for testing */
trait Time {
  def now(): Long
  def sleep(ms: Long)
}

object NormalTime extends Time {
  val clock = Clock.systemDefaultZone()

  override def now() = clock.millis()

  override def sleep(ms: Long) {
    Thread.sleep(ms)
  }
}
