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

package uk.gov.hmrc.secure

import org.scalatest.FunSuite

class PasswordHasherTest extends FunSuite {

  test("For a variety of strings, slowEquals should return true when both parameters are the same") {
    import PasswordHasher._
    assert(slowEquals("", "") === true)
    assert(slowEquals("a", "a") === true)
    assert(slowEquals("A Much Larger String Containing µø¢ etc", "A Much Larger String Containing µø¢ etc") === true)
  }

  test("For some similar but different strings, slowEquals should return false") {
    import PasswordHasher._
    assert(slowEquals("", "1") === false)
    assert(slowEquals("a", "b") === false)
    assert(slowEquals("A Much Larger String Containing ¢µø etc", "A Much Larger String Containing µø¢ etc") === false)
  }

  test("should create random salts with a different output each time it is called") {
    val s1 = PasswordHasher.newRandomSalt(10)
    val s2 = PasswordHasher.newRandomSalt(10)
    val s3 = PasswordHasher.newRandomSalt(10)
    assert(s1 !== "")
    assert(s1 !== s2)
    assert(s3 !== s2)
    assert(s3 !== s1)
    assert(s1.value.length === 16)
  }

  test("should create random-salted hash with length determined by the constructor parameter") {
    val s1 = PasswordHasher.newRandomSalt(12)
    assert(s1.value.length === 16)

    val s2 = PasswordHasher.newRandomSalt(24)
    assert(s2.value.length === 32)
  }

  test("should create consistent hashes using a given salt") {
    val gen = new PasswordHasher(1, 1)
    val s1 = gen.withSalt(Salt("salt")).hash("foo")
    val s2 = gen.withSalt(Salt("salt")).hash("foo")
    val s3 = gen.withSalt(Salt("salt")).hash("foo")
    assert(s1 !== "foo")
    assert(s1 === s2)
    assert(s1 === s3)
  }

  test("should create specific-salted hash with length determined by the constructor parameter") {
    val gen1 = new PasswordHasher(12, 5)
    val h1 = gen1.withSalt(Salt("salt")).hash("foo")
    assert(h1.value.length === 16)
    assert(h1 !== "foo")

    val gen2 = new PasswordHasher(24, 5)
    val h2 = gen2.withSalt(Salt("salt")).hash("foo")
    assert(h2.value.length === 32)
    assert(h2 !== "foo")
  }

  test("should deal with very small parameters ok") {
    val s1 = PasswordHasher.newRandomSalt(1)
    assert(s1.value.length === 4)
    assert(s1 !== "foo")

    val gen1 = new PasswordHasher(1, 1)
    val s2 = gen1.withSalt(Salt("salt")).hash("foo")
    assert(s2.value.length === 4)
    assert(s2 !== "foo")
  }
}

