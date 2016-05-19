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

package ingest

import java.io.File

// scalastyle:off

object LoadZipEssay extends App {

  val home = System.getenv("HOME")
  val sample = new File(home + "/OSGB-AddressBasePremium/abp/40/full/DVD2.zip")
  val zip = LoadZip.zipReader(sample, n => n.endsWith(".csv"))
  assert(zip.hasNext)
  val it = zip.next
  expect(it.zipEntry.getName, "AddressBasePremium_FULL_2016-04-07_191.csv")
  assert(it.hasNext)
  expect(it.next.mkString(","), "10,NAG Hub - GeoPlace,9999,2016-04-04,191,2016-04-04,12:27:39,2.0,F")
  expect(it.next.mkString(","), "23,I,1,90004312,4615X900007992,osgb1000002247615045,5,7666MA,2016-02-07,,2016-02-10,2007-01-01")
  expect(it.next.mkString(","), "23,I,2,90004313,4615X900007993,osgb1000002247615046,5,7666MA,2016-02-07,,2016-02-10,2007-01-01")
  var i = 3
  while (i < 102300) {
    assert(it.hasNext, i)
    it.next
    i += 1
  }
  while (i < 999990) {
    assert(it.hasNext, i)
    val row = it.next
    println(row.mkString(","))
    val procOrder = row(2)
    expect(procOrder, i.toString)
    i += 1
  }
  assert(it.hasNext)
  expect(it.next.mkString(","), "23,I,1000000,452040986,3240X900033974,osgb1000002208111199,6,7666MA,2016-02-07,,2016-02-10,2007-01-01")
  expect(it.next.mkString(","), "29,AddressBase Premium,BLPUs, Delivery Points, Streets and associated information,England, Wales and Scotland,ADDRESS LAYER 2, NLPG, PAF, VOA CT and VOA NDR, Code-Point Polygons and Boundary-Line,GeoPlace,S,GeoPlace,100023346366,9999,British National Grid,Metres,2016-03-21,AddressBase Premium Classification Scheme version 1.0,2016-03-21,BIL,UTF-8\"")
  expect(it.next.mkString(","), "99,192,1000000,2016-04-04,12:28:14")
  assert(!it.hasNext)

  assert(!zip.hasNext)
  zip.close()

  def expect(s1: String, exp: String) {
    assert(s1 == exp, s"want $exp, got $s1")
  }
}
