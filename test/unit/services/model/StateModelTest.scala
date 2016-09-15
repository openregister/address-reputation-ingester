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

package services.model

import java.net.URL

import fetch.{OSGBProduct, WebDavFile}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}
import services.db.CollectionName

@RunWith(classOf[JUnitRunner])
class StateModelTest extends FunSuite with Matchers {

  val base = "http://somedavserver.com:81/webdav"
  val baseUrl = new URL(base + "/")

  test(
    """
      StateModel.apply correctly converts an OSGBProduct
    """) {
    val f1 = WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true)
    val p = OSGBProduct("abp", 38, List(f1))
    val m = StateModel(p)

    m should be(StateModel("abp", 38, None, None,  Some(p)))
  }

  test(
    """
      StateModel.apply correctly converts a CollectionName
    """) {
    val f1 = WebDavFile(new URL(base + "/abp/38/full/DVD1.zip"), "DVD1.zip", isDataFile = true)
    val c = CollectionName("abp", Some(38), Some("ts1"))
    val m = StateModel(c)

    m should be(StateModel("abp", 38, None, Some("ts1")))
  }

}
