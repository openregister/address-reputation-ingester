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

import org.scalatest.FunSuite
import uk.co.hmrc.logging.StubLogger

class StatusLoggerTest extends FunSuite {

  test(
    """
      When messages are put onto the statusLogger
      Then messages are updated in the statusLogger
      The status will return them
    """) {

    val tee = new StubLogger

    val statusLogger = new StatusLogger(tee)

    statusLogger.info("first")
    assert(statusLogger.status === "first")

    statusLogger.info("second")
    assert(statusLogger.status === "first\nsecond")

    statusLogger.update("third")
    assert(statusLogger.status === "first\nsecond\nthird")

    statusLogger.update("fourth")
    assert(statusLogger.status === "first\nsecond\nfourth")

    statusLogger.info("fifth")
    assert(statusLogger.status === "first\nsecond\nfifth")

    statusLogger.update("sixth")
    assert(statusLogger.status === "first\nsecond\nfifth\nsixth")
  }

}
