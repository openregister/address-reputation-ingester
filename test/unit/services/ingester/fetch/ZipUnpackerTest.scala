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
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package services.ingester.fetch

import java.io.File

import org.scalatest.FunSuite

class ZipUnpackerTest extends FunSuite {

  test(
    """
       Given a zip file that contains nested zip files,
       Then unpack will expand the contents into a subdirectory.
    """) {
//    val sample = new File(getClass.getClassLoader.getResource("nested.zip").getFile)
//    val unpacked = ZipUnpacker.unpack(sample)
//    assert(unpacked === 2)
  }

  test(
    """
       Given a zip file that doesn't contain nested zip files,
       Then unpack will do nothing.
    """) {
//    val sample = new File(getClass.getClassLoader.getResource("3files.zip").getFile)
//    val unpacked = ZipUnpacker.unpack(sample)
//    assert(unpacked === 0)
  }

  test(
    """
       Given a file that isn't a zip file,
       Then unpack will do nothing.
    """) {
//    val unpacked = ZipUnpacker.unpack(new File("x"))
//    assert(unpacked === 0)
  }
}
