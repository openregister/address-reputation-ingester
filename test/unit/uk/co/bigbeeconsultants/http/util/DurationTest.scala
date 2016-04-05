//-----------------------------------------------------------------------------
// The MIT License
//
// Copyright (c) 2012 Rick Beton <rick@bigbeeconsultants.co.uk>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//-----------------------------------------------------------------------------

// see https://bitbucket.org/rickb777/bee-client

package uk.co.bigbeeconsultants.http.util

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class DurationTest extends FunSuite {

  test("+ 1") {
    val sum = Duration(333) + Duration(222)
    assert(sum.microseconds === 555L)
  }

  test("+ 2") {
    val sum = Duration(333) + 222
    assert(sum.microseconds === 555L)
  }

  test("- 1") {
    val sum = Duration(333) - Duration(222)
    assert(sum.microseconds === 111L)
  }

  test("- 2") {
    val sum = Duration(333) - 222
    assert(sum.microseconds === 111L)
  }

  test("*") {
    val sum = Duration(123) * 2
    assert(sum.microseconds === 246L)
  }

  test("/") {
    val sum = Duration(246) / 2
    assert(sum.microseconds === 123L)
  }

  test("max") {
    val max1 = Duration(246) max Duration(222)
    assert(max1.microseconds === 246L)
    val max2 = Duration(222) max Duration(246)
    assert(max2.microseconds === 246L)
  }

  test("min") {
    val min1 = Duration(246) min Duration(222)
    assert(min1.microseconds === 222L)
    val min2 = Duration(222) min Duration(246)
    assert(min2.microseconds === 222L)
  }

  test("abs") {
    assert(Duration(123).abs.microseconds === 123L)
    assert(Duration(-123).abs.microseconds === 123L)
  }

  test("toString") {
    assert(Duration(-1).toString === "-1µs")
    assert(Duration(0).toString === "0µs")
    assert(Duration(1).toString === "1µs")
    assert(Duration(5).toString === "5µs")
    assert(Duration(12).toString === "12µs")
    assert(Duration(51).toString === "51µs")
    assert(Duration(123).toString === "123µs")
    assert(Duration(512).toString === "512µs")
    assert(Duration(777).toString === "777µs")
    assert(Duration(12345L).toString === "12.3ms")
    assert(Duration(51234L).toString === "51.2ms")
    assert(Duration(51266L).toString === "51.3ms")
    assert(Duration(123456L).toString === "123ms")
    assert(Duration(512345L).toString === "512ms")
    assert(Duration(777777L).toString === "778ms")
    assert(Duration(1234321L).toString === "1234ms")
    assert(Duration(5123456L).toString === "5123ms")
    assert(Duration(7777777L).toString === "7778ms")
    assert(Duration(12345678L).toString === "12.3s")
    assert(Duration(51234567L).toString === "51.2s")
    assert(Duration(51266666L).toString === "51.3s")
    assert(Duration(77777777L).toString === "77.8s")
  }
}
