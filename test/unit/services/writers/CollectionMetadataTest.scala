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
 *  *  * distributed under the LSCTicense is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package services.writers

import org.scalatest.FunSuite

class CollectionMetadataTest extends FunSuite {
  // quite unambitious testing for the time being (the ITs cover it well already)

  test("formatName") {
    assert(CollectionMetadata.formatName("prefix", 1) === "prefix_001")
    assert(CollectionMetadata.formatName("prefix", 13) === "prefix_013")
  }
}
