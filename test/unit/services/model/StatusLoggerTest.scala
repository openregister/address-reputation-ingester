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
 */

package services.model

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import uk.gov.hmrc.logging.StubLogger

@RunWith(classOf[JUnitRunner])
class StatusLoggerTest extends FunSuite with Matchers {

  test(
    """
      When messages are put onto the statusLogger
      Then messages are updated in the statusLogger
      The status will return them
    """) {

    val tee = new StubLogger

    val statusLogger = new StatusLogger(tee, 2)

    statusLogger.info("a1")
    statusLogger.status should fullyMatch regex "a1"

    statusLogger.info("a2")
    statusLogger.status should fullyMatch regex "a1\na2"

    statusLogger.startAfresh()
    statusLogger.info("b3")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3"

    statusLogger.info("b4")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3\nb4"

    statusLogger.info("b5")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3\nb4\nb5"

    statusLogger.info("b6")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3\nb4\nb5\nb6"

    statusLogger.startAfresh()
    statusLogger.info("c7")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3\nb4\nb5\nb6\n~~~~~~~~~~~~~~~\nc7"

    statusLogger.info("c8")
    statusLogger.status should fullyMatch regex "a1\na2\n~~~~~~~~~~~~~~~\nb3\nb4\nb5\nb6\n~~~~~~~~~~~~~~~\nc7\nc8"

    statusLogger.startAfresh()
    statusLogger.info("d9")
    statusLogger.status should fullyMatch regex "b3\nb4\nb5\nb6\n~~~~~~~~~~~~~~~\nc7\nc8\n~~~~~~~~~~~~~~~\nd9"

    statusLogger.info("d10")
    statusLogger.status should fullyMatch regex "b3\nb4\nb5\nb6\n~~~~~~~~~~~~~~~\nc7\nc8\n~~~~~~~~~~~~~~~\nd9\nd10"

    statusLogger.startAfresh()
    statusLogger.info("e11")
    statusLogger.status should fullyMatch regex "c7\nc8\n~~~~~~~~~~~~~~~\nd9\nd10\n~~~~~~~~~~~~~~~\ne11"

    statusLogger.info("e12")
    statusLogger.status should fullyMatch regex "c7\nc8\n~~~~~~~~~~~~~~~\nd9\nd10\n~~~~~~~~~~~~~~~\ne11\ne12"
  }

}
