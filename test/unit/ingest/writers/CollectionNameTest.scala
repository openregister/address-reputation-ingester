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

package ingest.writers

import org.scalatest.FunSuite

class CollectionNameTest extends FunSuite {

  test("format CollectionName") {
    assert(CollectionName("fooey", Some(1), Some(2)).toString === "fooey_1_002")
    assert(CollectionName("fooey", Some(40), Some(13)).toString === "fooey_40_013")
    assert(CollectionName("fooey", Some(40), None).toString === "fooey_40")
    assert(CollectionName("fooey", None, None).toString === "fooey")
  }

  test("CollectionName prefix") {
    assert(CollectionName("fooey", Some(1), Some(2)).toPrefix === "fooey_1")
    assert(CollectionName("fooey", Some(40), None).toPrefix === "fooey_40")
  }

  test("parse via apply") {
    assert(CollectionName("") === None)
    assert(CollectionName("foo") === Some(CollectionName("foo", None, None)))
    assert(CollectionName("foo_bar") === None)
    assert(CollectionName("foo_bar_baz") === None)
    assert(CollectionName("abp_40") === Some(CollectionName("abp", Some(40), None)))
    assert(CollectionName("abp_40_002") === Some(CollectionName("abp", Some(40), Some(2))))
    assert(CollectionName("abp_40_002_zz") === None)
    assert(CollectionName("abp_40_foo") === None)
    assert(CollectionName("abp_foo_002") === None)
  }
}
