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

package services.ingester.converter

import java.io.File

import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FunSuite, Matchers}
import uk.co.hmrc.address.osgb.DbAddress

import scala.util.Success

class ExtractorTest extends FunSuite with Matchers with MockitoSugar {

  val dummyOut = (out: DbAddress) => {}


  test("string processing functions ") {
    import OSCleanup._

    assert("  ".cleanup === "")
    assert("\"\"".cleanup === "")
    assert("\"Hello\"".cleanup === "Hello")
  }


  test("Having no files should successfully return nothing") {
    val mockFile = mock[File]

    when(mockFile.isDirectory) thenReturn true
    when(mockFile.listFiles) thenReturn Array.empty[File]

    val result = Extractor.extract(mockFile, dummyOut)

    assert(result === Success(0))
  }

}
