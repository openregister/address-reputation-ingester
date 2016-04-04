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

package services.ingester.converter.extractor

import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import services.ingester.converter.Extractor.Blpu

import scala.collection.immutable.{HashMap, HashSet}

class ForwardDataTest extends FunSuite with Matchers with MockitoSugar {


  test("Check the combining of two empty ForwardData objects works") {
    val fd1 = ForwardData.empty
    val fd2 = ForwardData.empty

    val expectedResult = ForwardData.empty

    val result = fd1.update(fd2)

    assert(result === expectedResult)
  }


  test("Check the combining of two ForwardData objects works") {
    val fd1 = ForwardData.empty

    val blpu1 = 1L -> Blpu("", 'a')

    val fd2 = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](blpu1))

    val result = fd1.update(fd2)

    val expectedResult = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](blpu1))

    assert(result === expectedResult)
  }


  test("Check the combining two ForwardData will remove item from dpa forward references") {
    val fd1 = ForwardData.empty.copy(dpa = HashSet[Long](1L))

    val fd2 = ForwardData.empty.copy(blpu = HashMap[Long, Blpu](1L -> Blpu("", 'a')))

    val result = fd1.update(fd2)

    val expectedResult = ForwardData.empty

    assert(result === expectedResult)
  }


  test("all things ForwardData") {
    val fd1 = ForwardData.empty

    assert(fd1.toString === "ForwardData(Map(),Set(),Map(),Map())")
  }

}
