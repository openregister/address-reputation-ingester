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

class ExtractorTest extends FunSuite with Matchers with MockitoSugar {

  val dummyOut = (out: DbAddress) => {}


  test("Having no files should not throw any exception") {
    val mockFile = mock[File]

    when(mockFile.isDirectory) thenReturn true
    when(mockFile.listFiles) thenReturn Array.empty[File]

    Extractor.extract(mockFile, dummyOut)
  }

}
