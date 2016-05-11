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

package services.fetch

import java.io.File

import org.scalatest.{BeforeAndAfterAll, FunSuite}
import services.fetch.Utils._
import services.model.StatusLogger
import uk.co.hmrc.logging.StubLogger

class ZipUnpackerTest extends FunSuite with BeforeAndAfterAll {

  val tempDir = new File(System.getProperty("java.io.tmpdir") + "/zip-unpacker-test")

  override def afterAll() {
    deleteDir(tempDir)
  }

  test(
    """
       Given a zip file that contains nested zip files,
       Then unpack will expand the contents into a subdirectory,
       And any pre-existing files will be deleted.
    """) {
    deleteDir(tempDir)
    // given
    val fooBar = new File(tempDir, "foo/bar")
    fooBar.mkdirs()
    val preExisting = new File(fooBar, "preExisting.txt")
    assert(preExisting.createNewFile())

    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val sample = new File(getClass.getClassLoader.getResource("nested.zip").getFile)

    // when
    val unzipped = new ZipUnpacker(tempDir).unzipList(List(sample), "foo/bar", status)

    // then
    assert(unzipped === 2)
    val e1 = new File(tempDir, "foo/bar/data/SX9090-first3600.zip")
    val e2 = new File(tempDir, "foo/bar/resources/hello.txt")
    assert(e1.exists, e1)
    assert(e2.exists, e2)
    assert(!preExisting.exists(), preExisting)
  }

  test(
    """
       Given a zip file that doesn't contain nested zip files,
       Then unpack will do nothing.
    """) {
    deleteDir(tempDir)
    // given
    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val sample = new File(getClass.getClassLoader.getResource("SX9090-first20.zip").getFile)

    // when
    val unzipped = new ZipUnpacker(tempDir).unzip(sample, "foo", status)

    // then
    assert(unzipped === 1)

    val e1 = new File(tempDir, "foo/SX9090-first20.csv")
    assert(e1.exists, e1)
  }

  test(
    """
       Given a file that isn't a zip file,
       Then unpack will do nothing.
    """) {
    deleteDir(tempDir)
    // given
    val logger = new StubLogger
    val status = new StatusLogger(logger)
    val sample = new File(getClass.getClassLoader.getResource("invalid15.csv").getFile)

    //when
    val unzipped = new ZipUnpacker(tempDir).unzip(sample, "foo", status)

    // then
    assert(unzipped === 0)
  }
}
