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

package config

import org.scalatest.FunSuite

class DividerTest extends FunSuite {

  test("divide") {
    assert("".divide('|') === List(""))
    assert("xx".divide('|') === List("xx"))
    assert("xx|".divide('|') === List("xx", ""))
    assert("xx|yy".divide('|') === List("xx", "yy"))
    assert("|yy".divide('|') === List("", "yy"))
    assert("xx|yy|zz".divide('|') === List("xx", "yy|zz"))
  }

  test("divideLast") {
    assert("".divideLast('|') === List(""))
    assert("xx".divideLast('|') === List("xx"))
    assert("xx|".divideLast('|') === List("xx", ""))
    assert("xx|yy".divideLast('|') === List("xx", "yy"))
    assert("|yy".divideLast('|') === List("", "yy"))
    assert("xx|yy|zz".divideLast('|') === List("xx|yy", "zz"))
  }

  test("qsplit") {
    assert("".qsplit('|') === List(""))
    assert("xx".qsplit('|') === List("xx"))
    assert("xx|".qsplit('|') === List("xx", ""))
    assert("xx|yy".qsplit('|') === List("xx", "yy"))
    assert("|yy".qsplit('|') === List("", "yy"))
    assert("ww|xx|yy|zz".qsplit('|') === List("ww", "xx", "yy", "zz"))
  }
}
